package org.elasticsearch.dsl.parser;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.*;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.elasticsearch.dsl.ElasticDslContext;
import org.elasticsearch.dsl.exception.ElasticSql2DslException;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.sql.ElasticSqlSelectQueryBlock;

import java.util.List;

public class QueryWhereConditionParser {
    public static void parseFilterCondition(ElasticDslContext dslContext) {
        ElasticSqlSelectQueryBlock queryBlock = (ElasticSqlSelectQueryBlock) dslContext.getQueryExpr().getSubQuery().getQuery();
        if (queryBlock.getWhere() != null) {
            FilterBuilder sqlWhereFilter = parseFilterCondition(dslContext, queryBlock.getWhere());
            dslContext.boolFilter().must(sqlWhereFilter);
        }
    }

    private static FilterBuilder parseFilterCondition(ElasticDslContext dslContext, SQLExpr sqlExpr) {
        if (sqlExpr instanceof SQLBinaryOpExpr) {
            SQLBinaryOpExpr sqlBinOpExpr = (SQLBinaryOpExpr) sqlExpr;
            SQLBinaryOperator binaryOperator = sqlBinOpExpr.getOperator();

            if (ElasticSqlParseUtil.isValidBinOperator(binaryOperator)) {
                //AND OR
                if (SQLBinaryOperator.BooleanAnd == binaryOperator || SQLBinaryOperator.BooleanOr == binaryOperator) {
                    FilterBuilder leftFilter = parseFilterCondition(dslContext, sqlBinOpExpr.getLeft());
                    FilterBuilder rightFilter = parseFilterCondition(dslContext, sqlBinOpExpr.getRight());
                    if (SQLBinaryOperator.BooleanAnd == binaryOperator) {
                        return FilterBuilders.boolFilter().must(leftFilter).must(rightFilter);
                    } else {
                        return FilterBuilders.boolFilter().should(leftFilter).should(rightFilter);
                    }
                }

                //以下是二元运算,左边必须是变量
                if (!(sqlBinOpExpr.getLeft() instanceof SQLPropertyExpr || sqlBinOpExpr.getLeft() instanceof SQLIdentifierExpr)) {
                    throw new ElasticSql2DslException("[syntax error] Equality expr left part should be a param name");
                }

                //EQ NEQ
                if (SQLBinaryOperator.Equality == binaryOperator || SQLBinaryOperator.LessThanOrGreater == binaryOperator || SQLBinaryOperator.NotEqual == binaryOperator) {
                    Object targetVal = ElasticSqlParseUtil.transferSqlArg(sqlBinOpExpr.getRight());
                    return parseCondition(sqlBinOpExpr.getLeft(), dslContext.getQueryAs(), idfName -> {
                        FilterBuilder eqFilter = FilterBuilders.termFilter(idfName, targetVal);
                        if (SQLBinaryOperator.Equality == binaryOperator) {
                            return eqFilter;
                        } else {
                            return FilterBuilders.notFilter(eqFilter);
                        }
                    });
                }

                //GT GTE LT LTE
                if (SQLBinaryOperator.GreaterThan == binaryOperator || SQLBinaryOperator.GreaterThanOrEqual == binaryOperator
                        || SQLBinaryOperator.LessThan == binaryOperator || SQLBinaryOperator.LessThanOrEqual == binaryOperator) {
                    Object targetVal = ElasticSqlParseUtil.transferSqlArg(sqlBinOpExpr.getRight());
                    return parseCondition(sqlBinOpExpr.getLeft(), dslContext.getQueryAs(), idfName -> {
                        FilterBuilder rangeFilter = null;
                        if (SQLBinaryOperator.GreaterThan == binaryOperator) {
                            rangeFilter = FilterBuilders.rangeFilter(idfName).gt(targetVal);
                        } else if (SQLBinaryOperator.GreaterThanOrEqual == binaryOperator) {
                            rangeFilter = FilterBuilders.rangeFilter(idfName).gte(targetVal);
                        } else if (SQLBinaryOperator.LessThan == binaryOperator) {
                            rangeFilter = FilterBuilders.rangeFilter(idfName).lt(targetVal);
                        } else if (SQLBinaryOperator.LessThanOrEqual == binaryOperator) {
                            rangeFilter = FilterBuilders.rangeFilter(idfName).lte(targetVal);
                        }
                        return rangeFilter;
                    });
                }

                //IS / IS NOT
                if (SQLBinaryOperator.Is == binaryOperator || SQLBinaryOperator.IsNot == binaryOperator) {
                    if (!(sqlBinOpExpr.getRight() instanceof SQLNullExpr)) {
                        throw new ElasticSql2DslException("[syntax error] Is/IsNot expr right part should be null");
                    }
                    return parseCondition(sqlBinOpExpr.getLeft(), dslContext.getQueryAs(), idfName -> {
                        FilterBuilder missingFilter = FilterBuilders.missingFilter(idfName);
                        if (SQLBinaryOperator.IsNot == binaryOperator) {
                            return FilterBuilders.notFilter(missingFilter);
                        }
                        return missingFilter;
                    });
                }
            }
        } else if (sqlExpr instanceof SQLInListExpr) {
            SQLInListExpr inListExpr = (SQLInListExpr) sqlExpr;
            if (CollectionUtils.isEmpty(inListExpr.getTargetList())) {
                throw new ElasticSql2DslException("[syntax error] In list expr target list cannot be blank");
            }
            Object[] targetInList = ElasticSqlParseUtil.transferSqlArgs(inListExpr.getTargetList());
            return parseCondition(inListExpr.getExpr(), dslContext.getQueryAs(), idfName -> {
                if (inListExpr.isNot()) {
                    return FilterBuilders.notFilter(FilterBuilders.inFilter(idfName, targetInList));
                } else {
                    return FilterBuilders.inFilter(idfName, targetInList);
                }
            });
        } else if (sqlExpr instanceof SQLBetweenExpr) {
            SQLBetweenExpr betweenExpr = (SQLBetweenExpr) sqlExpr;
            if ((!(betweenExpr.getBeginExpr() instanceof SQLIntegerExpr) && !(betweenExpr.getBeginExpr() instanceof SQLNumberExpr))
                    || (!(betweenExpr.getEndExpr() instanceof SQLIntegerExpr) && !(betweenExpr.getEndExpr() instanceof SQLNumberExpr))) {
                throw new ElasticSql2DslException(String.format("[syntax error] Can not support between arg type : begin >%s end> %s",
                        betweenExpr.getBeginExpr().getClass(), betweenExpr.getEndExpr().getClass()));
            }

            Object from = ElasticSqlParseUtil.transferSqlArg(betweenExpr.getBeginExpr());
            Object to = ElasticSqlParseUtil.transferSqlArg(betweenExpr.getEndExpr());

            return parseCondition(betweenExpr.getTestExpr(), dslContext.getQueryAs(), idfName -> {
                return FilterBuilders.rangeFilter(idfName).gte(from).lte(to);
            });
        }
        throw new ElasticSql2DslException("[syntax error] Can not support Op type: " + sqlExpr.getClass());
    }

    private static FilterBuilder parseCondition(SQLExpr sqlExpr, String queryAs, ConditionFilterBuilder filterBuilder) {
        List<FilterBuilder> tmpFilterList = Lists.newLinkedList();
        ElasticSqlIdfParser.parseSqlIdentifier(sqlExpr, queryAs, idfName -> {
            FilterBuilder originalFilter = filterBuilder.buildFilter(idfName);
            tmpFilterList.add(originalFilter);
        }, (nestPath, idfName) -> {
            FilterBuilder originalFilter = filterBuilder.buildFilter(idfName);
            FilterBuilder nestFilter = FilterBuilders.nestedFilter(nestPath, originalFilter);
            tmpFilterList.add(nestFilter);
        });
        if (CollectionUtils.isNotEmpty(tmpFilterList)) {
            return tmpFilterList.get(0);
        }
        return null;
    }

    @FunctionalInterface
    private interface ConditionFilterBuilder {
        FilterBuilder buildFilter(String idfName);
    }
}
