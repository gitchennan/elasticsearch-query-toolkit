package org.es.sql.dsl.parser.query.exact;

import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import com.alibaba.druid.sql.ast.expr.SQLNullExpr;
import org.elasticsearch.index.query.ExistsFilterBuilder;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.es.sql.dsl.bean.AtomFilter;
import org.es.sql.dsl.enums.SQLConditionOperator;
import org.es.sql.dsl.exception.ElasticSql2DslException;
import org.es.sql.dsl.helper.ElasticSqlArgTransferHelper;
import org.es.sql.dsl.listener.ParseActionListener;

public class BinaryAtomQueryParser extends AbstractAtomExactQueryParser {

    public BinaryAtomQueryParser(ParseActionListener parseActionListener) {
        super(parseActionListener);
    }

    public AtomFilter parseBinaryQuery(SQLBinaryOpExpr binQueryExpr, String queryAs, Object[] sqlArgs) {
        SQLBinaryOperator binaryOperator = binQueryExpr.getOperator();

        //EQ NEQ
        if (SQLBinaryOperator.Equality == binaryOperator || SQLBinaryOperator.LessThanOrGreater == binaryOperator || SQLBinaryOperator.NotEqual == binaryOperator) {
            Object targetVal = ElasticSqlArgTransferHelper.transferSqlArg(binQueryExpr.getRight(), sqlArgs);

            SQLConditionOperator operator = SQLBinaryOperator.Equality == binaryOperator ? SQLConditionOperator.Equality : SQLConditionOperator.NotEqual;

            return parseCondition(binQueryExpr.getLeft(), operator, new Object[]{targetVal}, queryAs, new IConditionExactQueryBuilder() {
                @Override
                public FilterBuilder buildQuery(String queryFieldName, SQLConditionOperator operator, Object[] rightParamValues) {
                    FilterBuilder eqQuery = FilterBuilders.termFilter(queryFieldName, rightParamValues[0]);
                    if (SQLConditionOperator.Equality == operator) {
                        return eqQuery;
                    }
                    else {
                        return FilterBuilders.boolFilter().mustNot(eqQuery);
                    }
                }
            });
        }

        //GT GTE LT LTE
        if (SQLBinaryOperator.GreaterThan == binaryOperator || SQLBinaryOperator.GreaterThanOrEqual == binaryOperator
                || SQLBinaryOperator.LessThan == binaryOperator || SQLBinaryOperator.LessThanOrEqual == binaryOperator) {

            SQLConditionOperator operator = null;
            if (SQLBinaryOperator.GreaterThan == binaryOperator) {
                operator = SQLConditionOperator.GreaterThan;
            }
            else if (SQLBinaryOperator.GreaterThanOrEqual == binaryOperator) {
                operator = SQLConditionOperator.GreaterThanOrEqual;
            }
            else if (SQLBinaryOperator.LessThan == binaryOperator) {
                operator = SQLConditionOperator.LessThan;
            }
            else if (SQLBinaryOperator.LessThanOrEqual == binaryOperator) {
                operator = SQLConditionOperator.LessThanOrEqual;
            }

            Object targetVal = ElasticSqlArgTransferHelper.transferSqlArg(binQueryExpr.getRight(), sqlArgs);
            return parseCondition(binQueryExpr.getLeft(), operator, new Object[]{targetVal}, queryAs, new IConditionExactQueryBuilder() {
                @Override
                public FilterBuilder buildQuery(String queryFieldName, SQLConditionOperator operator, Object[] rightParamValues) {
                    FilterBuilder rangeQuery = null;
                    if (SQLConditionOperator.GreaterThan == operator) {
                        rangeQuery = FilterBuilders.rangeFilter(queryFieldName).gt(rightParamValues[0]);
                    }
                    else if (SQLConditionOperator.GreaterThanOrEqual == operator) {
                        rangeQuery = FilterBuilders.rangeFilter(queryFieldName).gte(rightParamValues[0]);
                    }
                    else if (SQLConditionOperator.LessThan == operator) {
                        rangeQuery = FilterBuilders.rangeFilter(queryFieldName).lt(rightParamValues[0]);
                    }
                    else if (SQLConditionOperator.LessThanOrEqual == operator) {
                        rangeQuery = FilterBuilders.rangeFilter(queryFieldName).lte(rightParamValues[0]);
                    }
                    return rangeQuery;
                }
            });
        }

        //IS / IS NOT
        if (SQLBinaryOperator.Is == binaryOperator || SQLBinaryOperator.IsNot == binaryOperator) {
            if (!(binQueryExpr.getRight() instanceof SQLNullExpr)) {
                throw new ElasticSql2DslException("[syntax error] Is/IsNot expr right part should be null");
            }
            SQLConditionOperator operator = SQLBinaryOperator.Is == binaryOperator ? SQLConditionOperator.IsNull : SQLConditionOperator.IsNotNull;
            return parseCondition(binQueryExpr.getLeft(), operator, null, queryAs, new IConditionExactQueryBuilder() {
                @Override
                public FilterBuilder buildQuery(String queryFieldName, SQLConditionOperator operator, Object[] rightParamValues) {
                    ExistsFilterBuilder existsQuery = FilterBuilders.existsFilter(queryFieldName);
                    if (SQLConditionOperator.IsNull == operator) {
                        return FilterBuilders.boolFilter().mustNot(existsQuery);
                    }
                    return existsQuery;
                }
            });
        }

        throw new ElasticSql2DslException(String.format("[syntax error] Can not support binary query type[%s]", binQueryExpr.toString()));
    }
}
