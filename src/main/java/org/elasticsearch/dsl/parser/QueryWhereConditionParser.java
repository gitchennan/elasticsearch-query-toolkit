package org.elasticsearch.dsl.parser;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.*;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.elasticsearch.dsl.ElasticDslContext;
import org.elasticsearch.dsl.exception.ElasticSql2DslException;
import org.elasticsearch.index.query.BoolFilterBuilder;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.sql.ElasticSqlSelectQueryBlock;

import java.util.List;

public class QueryWhereConditionParser implements ElasticSqlParser {
    @Override
    public void parse(ElasticDslContext dslContext) {
        ElasticSqlSelectQueryBlock queryBlock = (ElasticSqlSelectQueryBlock) dslContext.getQueryExpr().getSubQuery().getQuery();
        if (queryBlock.getWhere() != null) {
            SqlCondition sqlCondition = parseFilterCondition(dslContext, queryBlock.getWhere());
            if (!sqlCondition.isAndOr()) {
                dslContext.boolFilter().must(sqlCondition.getFilterList().get(0));
            } else {
                sqlCondition.getFilterList().stream().forEach(filter -> {
                    if (sqlCondition.getOperator() == SQLBinaryOperator.BooleanAnd) {
                        dslContext.boolFilter().must(filter);
                    }
                    if (sqlCondition.getOperator() == SQLBinaryOperator.BooleanOr) {
                        dslContext.boolFilter().should(filter);
                    }
                });
            }
        }
    }

    private SqlCondition parseFilterCondition(ElasticDslContext dslContext, SQLExpr sqlExpr) {
        if (sqlExpr instanceof SQLBinaryOpExpr) {
            SQLBinaryOpExpr sqlBinOpExpr = (SQLBinaryOpExpr) sqlExpr;
            SQLBinaryOperator binaryOperator = sqlBinOpExpr.getOperator();
            if (SQLBinaryOperator.BooleanAnd == binaryOperator || SQLBinaryOperator.BooleanOr == binaryOperator) {
                SqlCondition leftCondition = parseFilterCondition(dslContext, sqlBinOpExpr.getLeft());
                SqlCondition rightCondition = parseFilterCondition(dslContext, sqlBinOpExpr.getRight());

                List<FilterBuilder> curFilterList = Lists.newArrayList();

                if (!leftCondition.isAndOr() || leftCondition.operator == binaryOperator) {
                    curFilterList.addAll(leftCondition.getFilterList());
                } else {
                    BoolFilterBuilder subBoolFilter = FilterBuilders.boolFilter();
                    leftCondition.getFilterList().stream().forEach(filter -> {
                        if (leftCondition.getOperator() == SQLBinaryOperator.BooleanAnd) {
                            subBoolFilter.must(filter);
                        }
                        if (leftCondition.getOperator() == SQLBinaryOperator.BooleanOr) {
                            subBoolFilter.should(filter);
                        }
                    });
                    curFilterList.add(subBoolFilter);
                }

                if (!rightCondition.isAndOr() || rightCondition.operator == binaryOperator) {
                    curFilterList.addAll(rightCondition.getFilterList());
                } else {
                    BoolFilterBuilder subBoolFilter = FilterBuilders.boolFilter();
                    rightCondition.getFilterList().stream().forEach(filter -> {
                        if (rightCondition.getOperator() == SQLBinaryOperator.BooleanAnd) {
                            subBoolFilter.must(filter);
                        }
                        if (rightCondition.getOperator() == SQLBinaryOperator.BooleanOr) {
                            subBoolFilter.should(filter);
                        }
                    });
                    curFilterList.add(subBoolFilter);
                }
                return new SqlCondition(curFilterList, binaryOperator);
            }
        }
        return new SqlCondition(parseAtomFilterCondition(dslContext, sqlExpr));
    }

    private FilterBuilder parseAtomFilterCondition(ElasticDslContext dslContext, SQLExpr sqlExpr) {
        if (sqlExpr instanceof SQLBinaryOpExpr) {
            SQLBinaryOpExpr sqlBinOpExpr = (SQLBinaryOpExpr) sqlExpr;
            SQLBinaryOperator binaryOperator = sqlBinOpExpr.getOperator();

            if (ElasticSqlParseUtil.isValidBinOperator(binaryOperator)) {
                //以下是二元运算,左边必须是变量
                if (!(sqlBinOpExpr.getLeft() instanceof SQLPropertyExpr || sqlBinOpExpr.getLeft() instanceof SQLIdentifierExpr)) {
                    throw new ElasticSql2DslException("[syntax error] Binary operation expr left part should be a param name");
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

            Object from = ElasticSqlParseUtil.transferSqlArg(betweenExpr.getBeginExpr());
            Object to = ElasticSqlParseUtil.transferSqlArg(betweenExpr.getEndExpr());

            if (from == null || to == null) {
                throw new ElasticSql2DslException("[syntax error] Between Expr only support one of [number,date] arg type");
            }

            return parseCondition(betweenExpr.getTestExpr(), dslContext.getQueryAs(), idfName -> {
                return FilterBuilders.rangeFilter(idfName).gte(from).lte(to);
            });
        }
        throw new ElasticSql2DslException("[syntax error] Can not support Op type: " + sqlExpr.getClass());
    }

    private FilterBuilder parseCondition(SQLExpr sqlExpr, String queryAs, ConditionFilterBuilder filterBuilder) {
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

    private class SqlCondition {
        //是否AND/OR运算
        private boolean isAndOr = false;
        //运算符
        private SQLBinaryOperator operator;
        //条件集合
        private List<FilterBuilder> filterList;

        public SqlCondition(FilterBuilder atomFilter) {
            filterList = Lists.newArrayList(atomFilter);
            isAndOr = false;
        }

        public SqlCondition(List<FilterBuilder> filterList, SQLBinaryOperator operator) {
            this.filterList = filterList;
            isAndOr = true;
            this.operator = operator;
        }

        public boolean isAndOr() {
            return isAndOr;
        }

        public SQLBinaryOperator getOperator() {
            return operator;
        }

        public List<FilterBuilder> getFilterList() {
            return filterList;
        }
    }
}
