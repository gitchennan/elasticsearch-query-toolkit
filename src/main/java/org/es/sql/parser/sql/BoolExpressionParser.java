package org.es.sql.parser.sql;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.*;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.es.sql.bean.AtomQuery;
import org.es.sql.bean.SQLArgs;
import org.es.sql.bean.SQLCondition;
import org.es.sql.enums.SQLBoolOperator;
import org.es.sql.enums.SQLConditionType;
import org.es.sql.exception.ElasticSql2DslException;
import org.es.sql.listener.ParseActionListener;
import org.es.sql.listener.ParseActionListenerAdapter;
import org.es.sql.parser.query.exact.BetweenAndAtomQueryParser;
import org.es.sql.parser.query.exact.BinaryAtomQueryParser;
import org.es.sql.parser.query.exact.InListAtomQueryParser;
import org.es.sql.parser.query.method.MethodInvocation;
import org.es.sql.parser.query.method.fulltext.FullTextAtomQueryParser;
import org.es.sql.parser.query.method.join.JoinAtomQueryParser;
import org.es.sql.parser.query.method.script.ScriptAtomQueryParser;
import org.es.sql.parser.query.method.term.TermLevelAtomQueryParser;

import java.util.List;

public class BoolExpressionParser {

    private final TermLevelAtomQueryParser termLevelAtomQueryParser;
    private final ScriptAtomQueryParser scriptAtomQueryParser;
    private final FullTextAtomQueryParser fullTextAtomQueryParser;
    private final BinaryAtomQueryParser binaryQueryParser;
    private final InListAtomQueryParser inListQueryParser;
    private final BetweenAndAtomQueryParser betweenAndQueryParser;

    private final JoinAtomQueryParser joinAtomQueryParser;

    public BoolExpressionParser() {
        this(new ParseActionListenerAdapter());
    }

    public BoolExpressionParser(ParseActionListener parseActionListener) {
        termLevelAtomQueryParser = new TermLevelAtomQueryParser(parseActionListener);
        fullTextAtomQueryParser = new FullTextAtomQueryParser(parseActionListener);
        binaryQueryParser = new BinaryAtomQueryParser(parseActionListener);
        inListQueryParser = new InListAtomQueryParser(parseActionListener);
        betweenAndQueryParser = new BetweenAndAtomQueryParser(parseActionListener);

        scriptAtomQueryParser = new ScriptAtomQueryParser();
        joinAtomQueryParser = new JoinAtomQueryParser();
    }


    public BoolQueryBuilder parseBoolQueryExpr(SQLExpr conditionExpr, String queryAs, SQLArgs SQLArgs) {
        SQLCondition SQLCondition = recursiveParseBoolQueryExpr(conditionExpr, queryAs, SQLArgs);
        SQLBoolOperator operator = SQLCondition.getOperator();

        if (SQLConditionType.Atom == SQLCondition.getSQLConditionType()) {
            operator = SQLBoolOperator.AND;
        }
        return mergeAtomQuery(SQLCondition.getQueryList(), operator);
    }

    private SQLCondition recursiveParseBoolQueryExpr(SQLExpr conditionExpr, String queryAs, SQLArgs SQLArgs) {
        if (conditionExpr instanceof SQLBinaryOpExpr) {
            SQLBinaryOpExpr binOpExpr = (SQLBinaryOpExpr) conditionExpr;
            SQLBinaryOperator binOperator = binOpExpr.getOperator();

            if (SQLBinaryOperator.BooleanAnd == binOperator || SQLBinaryOperator.BooleanOr == binOperator) {
                SQLBoolOperator operator = SQLBinaryOperator.BooleanAnd == binOperator ? SQLBoolOperator.AND : SQLBoolOperator.OR;

                SQLCondition leftCondition = recursiveParseBoolQueryExpr(binOpExpr.getLeft(), queryAs, SQLArgs);
                SQLCondition rightCondition = recursiveParseBoolQueryExpr(binOpExpr.getRight(), queryAs, SQLArgs);

                List<AtomQuery> mergedQueryList = Lists.newArrayList();
                combineQueryBuilder(mergedQueryList, leftCondition, operator);
                combineQueryBuilder(mergedQueryList, rightCondition, operator);

                return new SQLCondition(mergedQueryList, operator);
            }
        }
        else if (conditionExpr instanceof SQLNotExpr) {
            SQLCondition innerSQLCondition = recursiveParseBoolQueryExpr(((SQLNotExpr) conditionExpr).getExpr(), queryAs, SQLArgs);

            SQLBoolOperator operator = innerSQLCondition.getOperator();
            if (SQLConditionType.Atom == innerSQLCondition.getSQLConditionType()) {
                operator = SQLBoolOperator.AND;
            }

            BoolQueryBuilder boolQuery = mergeAtomQuery(innerSQLCondition.getQueryList(), operator);
            boolQuery = QueryBuilders.boolQuery().mustNot(boolQuery);

            return new SQLCondition(new AtomQuery(boolQuery), SQLConditionType.Atom);
        }

        return new SQLCondition(parseAtomQueryCondition(conditionExpr, queryAs, SQLArgs), SQLConditionType.Atom);
    }

    private AtomQuery parseAtomQueryCondition(SQLExpr sqlConditionExpr, String queryAs, SQLArgs sqlArgs) {
        if (sqlConditionExpr instanceof SQLMethodInvokeExpr) {
            SQLMethodInvokeExpr methodQueryExpr = (SQLMethodInvokeExpr) sqlConditionExpr;

            MethodInvocation methodInvocation = new MethodInvocation(methodQueryExpr, queryAs, sqlArgs);

            if (scriptAtomQueryParser.isMatchMethodInvocation(methodInvocation)) {
                return scriptAtomQueryParser.parseAtomMethodQuery(methodInvocation);
            }

            if (fullTextAtomQueryParser.isFulltextAtomQuery(methodInvocation)) {
                return fullTextAtomQueryParser.parseFullTextAtomQuery(methodQueryExpr, queryAs, sqlArgs);
            }

            if (termLevelAtomQueryParser.isTermLevelAtomQuery(methodInvocation)) {
                return termLevelAtomQueryParser.parseTermLevelAtomQuery(methodQueryExpr, queryAs, sqlArgs);
            }

            if (joinAtomQueryParser.isJoinAtomQuery(methodInvocation)) {
                return joinAtomQueryParser.parseJoinAtomQuery(methodQueryExpr, queryAs, sqlArgs);
            }
        }
        else if (sqlConditionExpr instanceof SQLBinaryOpExpr) {
            return binaryQueryParser.parseBinaryQuery((SQLBinaryOpExpr) sqlConditionExpr, queryAs, sqlArgs);
        }
        else if (sqlConditionExpr instanceof SQLInListExpr) {
            return inListQueryParser.parseInListQuery((SQLInListExpr) sqlConditionExpr, queryAs, sqlArgs);
        }
        else if (sqlConditionExpr instanceof SQLBetweenExpr) {
            return betweenAndQueryParser.parseBetweenAndQuery((SQLBetweenExpr) sqlConditionExpr, queryAs, sqlArgs);
        }
        throw new ElasticSql2DslException(String.format("[syntax error] Can not support query condition type[%s]", sqlConditionExpr.toString()));
    }

    private void combineQueryBuilder(List<AtomQuery> combiner, SQLCondition SQLCondition, SQLBoolOperator binOperator) {
        if (SQLConditionType.Atom == SQLCondition.getSQLConditionType() || SQLCondition.getOperator() == binOperator) {
            combiner.addAll(SQLCondition.getQueryList());
        }
        else {
            BoolQueryBuilder boolQuery = mergeAtomQuery(SQLCondition.getQueryList(), SQLCondition.getOperator());
            combiner.add(new AtomQuery(boolQuery));
        }
    }

    private BoolQueryBuilder mergeAtomQuery(List<AtomQuery> atomQueryList, SQLBoolOperator operator) {
        BoolQueryBuilder subBoolQuery = QueryBuilders.boolQuery();
        ListMultimap<String, QueryBuilder> listMultiMap = ArrayListMultimap.create();

        for (AtomQuery atomQuery : atomQueryList) {
            if (Boolean.FALSE == atomQuery.getNestedQuery()) {
                if (operator == SQLBoolOperator.AND) {
                    subBoolQuery.must(atomQuery.getQuery());
                }
                if (operator == SQLBoolOperator.OR) {
                    subBoolQuery.should(atomQuery.getQuery());
                }
            }
            else {
                String nestedDocPrefix = atomQuery.getNestedQueryPath();
                listMultiMap.put(nestedDocPrefix, atomQuery.getQuery());
            }
        }

        for (String nestedDocPrefix : listMultiMap.keySet()) {
            List<QueryBuilder> nestedQueryList = listMultiMap.get(nestedDocPrefix);

            if (nestedQueryList.size() == 1) {
                if (operator == SQLBoolOperator.AND) {
                    subBoolQuery.must(QueryBuilders.nestedQuery(nestedDocPrefix, nestedQueryList.get(0), ScoreMode.None));
                }
                if (operator == SQLBoolOperator.OR) {
                    subBoolQuery.should(QueryBuilders.nestedQuery(nestedDocPrefix, nestedQueryList.get(0), ScoreMode.None));
                }
                continue;
            }

            BoolQueryBuilder boolNestedQuery = QueryBuilders.boolQuery();
            for (QueryBuilder nestedQueryItem : nestedQueryList) {
                if (operator == SQLBoolOperator.AND) {
                    boolNestedQuery.must(nestedQueryItem);
                }
                if (operator == SQLBoolOperator.OR) {
                    boolNestedQuery.should(nestedQueryItem);
                }
            }

            if (operator == SQLBoolOperator.AND) {
                subBoolQuery.must(QueryBuilders.nestedQuery(nestedDocPrefix, boolNestedQuery, ScoreMode.None));
            }
            if (operator == SQLBoolOperator.OR) {
                subBoolQuery.should(QueryBuilders.nestedQuery(nestedDocPrefix, boolNestedQuery, ScoreMode.None));
            }

        }
        return subBoolQuery;
    }


}
