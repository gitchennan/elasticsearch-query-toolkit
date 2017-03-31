package org.elasticsearch.dsl.parser.sql;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.*;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import org.elasticsearch.dsl.bean.AtomQuery;
import org.elasticsearch.dsl.bean.SQLCondition;
import org.elasticsearch.dsl.enums.SQLBoolOperator;
import org.elasticsearch.dsl.enums.SQLConditionType;
import org.elasticsearch.dsl.exception.ElasticSql2DslException;
import org.elasticsearch.dsl.listener.ParseActionListener;
import org.elasticsearch.dsl.parser.query.exact.BetweenAndAtomQueryParser;
import org.elasticsearch.dsl.parser.query.exact.BinaryAtomQueryParser;
import org.elasticsearch.dsl.parser.query.exact.InListAtomQueryParser;
import org.elasticsearch.dsl.parser.query.method.MethodInvocation;
import org.elasticsearch.dsl.parser.query.method.fulltext.FullTextAtomQueryParser;
import org.elasticsearch.dsl.parser.query.method.script.ScriptAtomQueryParser;
import org.elasticsearch.dsl.parser.query.method.term.TermLevelAtomQueryParser;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.List;

public abstract class AbstractQueryConditionParser implements QueryParser {

    private final TermLevelAtomQueryParser termLevelAtomQueryParser;
    private final ScriptAtomQueryParser scriptAtomQueryParser;
    private final FullTextAtomQueryParser fullTextAtomQueryParser;
    private final BinaryAtomQueryParser binaryQueryParser;
    private final InListAtomQueryParser inListQueryParser;
    private final BetweenAndAtomQueryParser betweenAndQueryParser;

    public AbstractQueryConditionParser(ParseActionListener parseActionListener) {
        termLevelAtomQueryParser = new TermLevelAtomQueryParser(parseActionListener);
        scriptAtomQueryParser = new ScriptAtomQueryParser(parseActionListener);
        fullTextAtomQueryParser = new FullTextAtomQueryParser(parseActionListener);
        binaryQueryParser = new BinaryAtomQueryParser(parseActionListener);
        inListQueryParser = new InListAtomQueryParser(parseActionListener);
        betweenAndQueryParser = new BetweenAndAtomQueryParser(parseActionListener);
    }

    protected BoolQueryBuilder parseQueryConditionExpr(SQLExpr conditionExpr, String queryAs, Object[] sqlArgs) {
        SQLCondition sqlCondition = recursiveParseQueryCondition(conditionExpr, queryAs, sqlArgs);
        SQLBoolOperator operator = sqlCondition.getOperator();

        if (SQLConditionType.Atom == sqlCondition.getSQLConditionType()) {
            operator = SQLBoolOperator.AND;
        }
        return mergeAtomQuery(sqlCondition.getQueryList(), operator);
    }

    private SQLCondition recursiveParseQueryCondition(SQLExpr conditionExpr, String queryAs, Object[] sqlArgs) {
        if (conditionExpr instanceof SQLBinaryOpExpr) {
            SQLBinaryOpExpr binOpExpr = (SQLBinaryOpExpr) conditionExpr;
            SQLBinaryOperator binOperator = binOpExpr.getOperator();

            if (SQLBinaryOperator.BooleanAnd == binOperator || SQLBinaryOperator.BooleanOr == binOperator) {
                SQLBoolOperator operator = SQLBinaryOperator.BooleanAnd == binOperator ? SQLBoolOperator.AND : SQLBoolOperator.OR;

                SQLCondition leftCondition = recursiveParseQueryCondition(binOpExpr.getLeft(), queryAs, sqlArgs);
                SQLCondition rightCondition = recursiveParseQueryCondition(binOpExpr.getRight(), queryAs, sqlArgs);

                List<AtomQuery> mergedQueryList = Lists.newArrayList();
                combineQueryBuilder(mergedQueryList, leftCondition, operator);
                combineQueryBuilder(mergedQueryList, rightCondition, operator);

                return new SQLCondition(mergedQueryList, operator);
            }
        }
        else if (conditionExpr instanceof SQLNotExpr) {
            SQLCondition innerSqlCondition = recursiveParseQueryCondition(((SQLNotExpr) conditionExpr).getExpr(), queryAs, sqlArgs);

            SQLBoolOperator operator = innerSqlCondition.getOperator();
            if (SQLConditionType.Atom == innerSqlCondition.getSQLConditionType()) {
                operator = SQLBoolOperator.AND;
            }

            BoolQueryBuilder boolQuery = mergeAtomQuery(innerSqlCondition.getQueryList(), operator);
            boolQuery = QueryBuilders.boolQuery().mustNot(boolQuery);

            return new SQLCondition(new AtomQuery(boolQuery), SQLConditionType.Atom);
        }

        return new SQLCondition(parseAtomQueryCondition(conditionExpr, queryAs, sqlArgs), SQLConditionType.Atom);
    }

    private AtomQuery parseAtomQueryCondition(SQLExpr sqlConditionExpr, String queryAs, Object[] sqlArgs) {
        if (sqlConditionExpr instanceof SQLMethodInvokeExpr) {
            SQLMethodInvokeExpr methodQueryExpr = (SQLMethodInvokeExpr) sqlConditionExpr;

            MethodInvocation methodInvocation = new MethodInvocation(methodQueryExpr, queryAs, sqlArgs);

            if (scriptAtomQueryParser.isMatchMethodInvocation(methodInvocation)) {
                return scriptAtomQueryParser.parseAtomMethodQuery(methodInvocation);
            }

            if (fullTextAtomQueryParser.isFulltextAtomQuery(methodQueryExpr)) {
                return fullTextAtomQueryParser.parseFullTextAtomQuery(methodQueryExpr, queryAs, sqlArgs);
            }

            if (termLevelAtomQueryParser.isTermLevelAtomQuery(methodInvocation)) {
                return termLevelAtomQueryParser.parseTermLevelAtomQuery(methodQueryExpr, queryAs, sqlArgs);
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

    private void combineQueryBuilder(List<AtomQuery> combiner, SQLCondition sqlCondition, SQLBoolOperator binOperator) {
        if (SQLConditionType.Atom == sqlCondition.getSQLConditionType() || sqlCondition.getOperator() == binOperator) {
            combiner.addAll(sqlCondition.getQueryList());
        }
        else {
            BoolQueryBuilder boolQuery = mergeAtomQuery(sqlCondition.getQueryList(), sqlCondition.getOperator());
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
                String nestedDocPrefix = atomQuery.getNestedQueryPathContext();
                listMultiMap.put(nestedDocPrefix, atomQuery.getQuery());
            }
        }

        for (String nestedDocPrefix : listMultiMap.keySet()) {
            List<QueryBuilder> nestedQueryList = listMultiMap.get(nestedDocPrefix);

            if (nestedQueryList.size() == 1) {
                if (operator == SQLBoolOperator.AND) {
                    subBoolQuery.must(QueryBuilders.nestedQuery(nestedDocPrefix, nestedQueryList.get(0)));
                }
                if (operator == SQLBoolOperator.OR) {
                    subBoolQuery.should(QueryBuilders.nestedQuery(nestedDocPrefix, nestedQueryList.get(0)));
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
                subBoolQuery.must(QueryBuilders.nestedQuery(nestedDocPrefix, boolNestedQuery));
            }
            if (operator == SQLBoolOperator.OR) {
                subBoolQuery.should(QueryBuilders.nestedQuery(nestedDocPrefix, boolNestedQuery));
            }

        }
        return subBoolQuery;
    }


}
