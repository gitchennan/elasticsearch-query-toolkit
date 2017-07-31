package org.es.sql.parser.query.exact;

import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import com.alibaba.druid.sql.ast.expr.SQLNullExpr;
import org.elasticsearch.index.query.ExistsQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.es.sql.bean.AtomQuery;
import org.es.sql.bean.SQLArgs;
import org.es.sql.enums.SQLConditionOperator;
import org.es.sql.exception.ElasticSql2DslException;
import org.es.sql.helper.ElasticSqlArgConverter;
import org.es.sql.listener.ParseActionListener;

public class BinaryAtomQueryParser extends AbstractAtomExactQueryParser {

    public BinaryAtomQueryParser(ParseActionListener parseActionListener) {
        super(parseActionListener);
    }

    public AtomQuery parseBinaryQuery(SQLBinaryOpExpr binQueryExpr, String queryAs, SQLArgs sqlArgs) {
        SQLBinaryOperator binaryOperator = binQueryExpr.getOperator();

        //EQ NEQ
        if (SQLBinaryOperator.Equality == binaryOperator || SQLBinaryOperator.LessThanOrGreater == binaryOperator || SQLBinaryOperator.NotEqual == binaryOperator) {
            Object targetVal = ElasticSqlArgConverter.convertSqlArg(binQueryExpr.getRight(), sqlArgs);

            SQLConditionOperator operator = SQLBinaryOperator.Equality == binaryOperator ? SQLConditionOperator.Equality : SQLConditionOperator.NotEqual;

            return parseCondition(binQueryExpr.getLeft(), operator, new Object[]{targetVal}, queryAs, new IConditionExactQueryBuilder() {
                @Override
                public QueryBuilder buildQuery(String queryFieldName, SQLConditionOperator operator, Object[] rightParamValues) {
                    QueryBuilder eqQuery = QueryBuilders.termQuery(queryFieldName, rightParamValues[0]);
                    if (SQLConditionOperator.Equality == operator) {
                        return eqQuery;
                    }
                    else {
                        return QueryBuilders.boolQuery().mustNot(eqQuery);
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

            Object targetVal = ElasticSqlArgConverter.convertSqlArg(binQueryExpr.getRight(), sqlArgs);
            return parseCondition(binQueryExpr.getLeft(), operator, new Object[]{targetVal}, queryAs, new IConditionExactQueryBuilder() {
                @Override
                public QueryBuilder buildQuery(String queryFieldName, SQLConditionOperator operator, Object[] rightParamValues) {
                    QueryBuilder rangeQuery = null;
                    if (SQLConditionOperator.GreaterThan == operator) {
                        rangeQuery = QueryBuilders.rangeQuery(queryFieldName).gt(rightParamValues[0]);
                    }
                    else if (SQLConditionOperator.GreaterThanOrEqual == operator) {
                        rangeQuery = QueryBuilders.rangeQuery(queryFieldName).gte(rightParamValues[0]);
                    }
                    else if (SQLConditionOperator.LessThan == operator) {
                        rangeQuery = QueryBuilders.rangeQuery(queryFieldName).lt(rightParamValues[0]);
                    }
                    else if (SQLConditionOperator.LessThanOrEqual == operator) {
                        rangeQuery = QueryBuilders.rangeQuery(queryFieldName).lte(rightParamValues[0]);
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
                public QueryBuilder buildQuery(String queryFieldName, SQLConditionOperator operator, Object[] rightParamValues) {
                    ExistsQueryBuilder existsQuery = QueryBuilders.existsQuery(queryFieldName);
                    if (SQLConditionOperator.IsNull == operator) {
                        return QueryBuilders.boolQuery().mustNot(existsQuery);
                    }
                    return existsQuery;
                }
            });
        }

        throw new ElasticSql2DslException(String.format("[syntax error] Can not support binary query type[%s]", binQueryExpr.toString()));
    }
}
