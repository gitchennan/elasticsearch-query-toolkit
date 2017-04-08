package org.es.sql.dsl.parser.sql;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.*;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import org.elasticsearch.index.query.BoolFilterBuilder;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.es.sql.dsl.bean.AtomFilter;
import org.es.sql.dsl.bean.SQLCondition;
import org.es.sql.dsl.enums.SQLBoolOperator;
import org.es.sql.dsl.enums.SQLConditionType;
import org.es.sql.dsl.exception.ElasticSql2DslException;
import org.es.sql.dsl.listener.ParseActionListener;
import org.es.sql.dsl.parser.query.exact.BetweenAndAtomQueryParser;
import org.es.sql.dsl.parser.query.exact.BinaryAtomQueryParser;
import org.es.sql.dsl.parser.query.exact.InListAtomQueryParser;
import org.es.sql.dsl.parser.query.method.MethodInvocation;
import org.es.sql.dsl.parser.query.method.script.ScriptAtomQueryParser;
import org.es.sql.dsl.parser.query.method.term.TermLevelAtomQueryParser;

import java.util.List;

public abstract class AbstractQueryConditionParser implements QueryParser {

    private final BinaryAtomQueryParser binaryQueryParser;
    private final InListAtomQueryParser inListQueryParser;
    private final BetweenAndAtomQueryParser betweenAndQueryParser;

    private final ScriptAtomQueryParser scriptAtomQueryParser;
    private final TermLevelAtomQueryParser termLevelAtomQueryParser;

    public AbstractQueryConditionParser(ParseActionListener parseActionListener) {
        binaryQueryParser = new BinaryAtomQueryParser(parseActionListener);
        inListQueryParser = new InListAtomQueryParser(parseActionListener);
        betweenAndQueryParser = new BetweenAndAtomQueryParser(parseActionListener);

        scriptAtomQueryParser = new ScriptAtomQueryParser();

        termLevelAtomQueryParser = new TermLevelAtomQueryParser(parseActionListener);
    }

    protected BoolFilterBuilder parseQueryConditionExpr(SQLExpr conditionExpr, String queryAs, Object[] sqlArgs) {
        SQLCondition sqlCondition = recursiveParseQueryCondition(conditionExpr, queryAs, sqlArgs);
        SQLBoolOperator operator = sqlCondition.getOperator();

        if (SQLConditionType.Atom == sqlCondition.getSQLConditionType()) {
            operator = SQLBoolOperator.AND;
        }
        return mergeAtomQuery(sqlCondition.getFilterList(), operator);
    }

    private SQLCondition recursiveParseQueryCondition(SQLExpr conditionExpr, String queryAs, Object[] sqlArgs) {
        if (conditionExpr instanceof SQLBinaryOpExpr) {
            SQLBinaryOpExpr binOpExpr = (SQLBinaryOpExpr) conditionExpr;
            SQLBinaryOperator binOperator = binOpExpr.getOperator();

            if (SQLBinaryOperator.BooleanAnd == binOperator || SQLBinaryOperator.BooleanOr == binOperator) {
                SQLBoolOperator operator = SQLBinaryOperator.BooleanAnd == binOperator ? SQLBoolOperator.AND : SQLBoolOperator.OR;

                SQLCondition leftCondition = recursiveParseQueryCondition(binOpExpr.getLeft(), queryAs, sqlArgs);
                SQLCondition rightCondition = recursiveParseQueryCondition(binOpExpr.getRight(), queryAs, sqlArgs);

                List<AtomFilter> mergedQueryList = Lists.newArrayList();
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

            BoolFilterBuilder boolQuery = mergeAtomQuery(innerSqlCondition.getFilterList(), operator);
            boolQuery = FilterBuilders.boolFilter().mustNot(boolQuery);

            return new SQLCondition(new AtomFilter(boolQuery), SQLConditionType.Atom);
        }

        return new SQLCondition(parseAtomQueryCondition(conditionExpr, queryAs, sqlArgs), SQLConditionType.Atom);
    }

    private AtomFilter parseAtomQueryCondition(SQLExpr sqlConditionExpr, String queryAs, Object[] sqlArgs) {
        if (sqlConditionExpr instanceof SQLMethodInvokeExpr) {
            SQLMethodInvokeExpr methodQueryExpr = (SQLMethodInvokeExpr) sqlConditionExpr;

            MethodInvocation methodInvocation = new MethodInvocation(methodQueryExpr, queryAs, sqlArgs);

            if (scriptAtomQueryParser.isMatchMethodInvocation(methodInvocation)) {
                return scriptAtomQueryParser.parseAtomMethodQuery(methodInvocation);
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

    private void combineQueryBuilder(List<AtomFilter> combiner, SQLCondition sqlCondition, SQLBoolOperator binOperator) {
        if (SQLConditionType.Atom == sqlCondition.getSQLConditionType() || sqlCondition.getOperator() == binOperator) {
            combiner.addAll(sqlCondition.getFilterList());
        }
        else {
            BoolFilterBuilder boolQuery = mergeAtomQuery(sqlCondition.getFilterList(), sqlCondition.getOperator());
            combiner.add(new AtomFilter(boolQuery));
        }
    }

    private BoolFilterBuilder mergeAtomQuery(List<AtomFilter> atomQueryList, SQLBoolOperator operator) {
        BoolFilterBuilder subBoolQuery = FilterBuilders.boolFilter();
        ListMultimap<String, FilterBuilder> listMultiMap = ArrayListMultimap.create();

        for (AtomFilter atomQuery : atomQueryList) {
            if (Boolean.FALSE == atomQuery.getNestedFilter()) {
                if (operator == SQLBoolOperator.AND) {
                    subBoolQuery.must(atomQuery.getFilter());
                }
                if (operator == SQLBoolOperator.OR) {
                    subBoolQuery.should(atomQuery.getFilter());
                }
            }
            else {
                String nestedDocPrefix = atomQuery.getNestedFilterPathContext();
                listMultiMap.put(nestedDocPrefix, atomQuery.getFilter());
            }
        }

        for (String nestedDocPrefix : listMultiMap.keySet()) {
            List<FilterBuilder> nestedQueryList = listMultiMap.get(nestedDocPrefix);

            if (nestedQueryList.size() == 1) {
                if (operator == SQLBoolOperator.AND) {
                    subBoolQuery.must(FilterBuilders.nestedFilter(nestedDocPrefix, nestedQueryList.get(0)));
                }
                if (operator == SQLBoolOperator.OR) {
                    subBoolQuery.should(FilterBuilders.nestedFilter(nestedDocPrefix, nestedQueryList.get(0)));
                }
                continue;
            }

            BoolFilterBuilder boolNestedQuery = FilterBuilders.boolFilter();
            for (FilterBuilder nestedQueryItem : nestedQueryList) {
                if (operator == SQLBoolOperator.AND) {
                    boolNestedQuery.must(nestedQueryItem);
                }
                if (operator == SQLBoolOperator.OR) {
                    boolNestedQuery.should(nestedQueryItem);
                }
            }

            if (operator == SQLBoolOperator.AND) {
                subBoolQuery.must(FilterBuilders.nestedFilter(nestedDocPrefix, boolNestedQuery));
            }
            if (operator == SQLBoolOperator.OR) {
                subBoolQuery.should(FilterBuilders.nestedFilter(nestedDocPrefix, boolNestedQuery));
            }

        }
        return subBoolQuery;
    }
}
