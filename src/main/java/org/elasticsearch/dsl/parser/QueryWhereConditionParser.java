package org.elasticsearch.dsl.parser;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.*;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.elasticsearch.dsl.ElasticDslContext;
import org.elasticsearch.dsl.ElasticSqlParseUtil;
import org.elasticsearch.dsl.exception.ElasticSql2DslException;
import org.elasticsearch.dsl.parser.helper.ElasticSqlIdentifierHelper;
import org.elasticsearch.index.query.BoolFilterBuilder;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.sql.ElasticSqlSelectQueryBlock;

import java.util.List;
import java.util.function.Consumer;

public class QueryWhereConditionParser implements QueryParser {
    @Override
    public void parse(ElasticDslContext dslContext) {
        ElasticSqlSelectQueryBlock queryBlock = (ElasticSqlSelectQueryBlock) dslContext.getQueryExpr().getSubQuery().getQuery();
        if (queryBlock.getWhere() != null) {
            SqlCondition sqlCondition = parseFilterCondition(dslContext, queryBlock.getWhere());
            if (!sqlCondition.isAndOr()) {
                dslContext.getParseResult().boolFilter().must(sqlCondition.getFilterList().get(0));
            } else {
                for (FilterBuilder filter : sqlCondition.getFilterList()) {
                    if (sqlCondition.getOperator() == SQLBinaryOperator.BooleanAnd) {
                        dslContext.getParseResult().boolFilter().must(filter);
                    }
                    if (sqlCondition.getOperator() == SQLBinaryOperator.BooleanOr) {
                        dslContext.getParseResult().boolFilter().should(filter);
                    }
                }
            }
        }
    }

    private SqlCondition parseFilterCondition(ElasticDslContext dslContext, SQLExpr sqlExpr) {
        if (sqlExpr instanceof SQLBinaryOpExpr) {
            SQLBinaryOpExpr sqlBinOpExpr = (SQLBinaryOpExpr) sqlExpr;
            SQLBinaryOperator binaryOperator = sqlBinOpExpr.getOperator();
            if (SQLBinaryOperator.BooleanAnd == binaryOperator || SQLBinaryOperator.BooleanOr == binaryOperator) {
                final SqlCondition leftCondition = parseFilterCondition(dslContext, sqlBinOpExpr.getLeft());
                final SqlCondition rightCondition = parseFilterCondition(dslContext, sqlBinOpExpr.getRight());

                List<FilterBuilder> curFilterList = Lists.newArrayList();

                if (!leftCondition.isAndOr() || leftCondition.operator == binaryOperator) {
                    curFilterList.addAll(leftCondition.getFilterList());
                } else {
                    final BoolFilterBuilder subBoolFilter = FilterBuilders.boolFilter();
                    leftCondition.getFilterList().stream().forEach(new Consumer<FilterBuilder>() {
                        @Override
                        public void accept(FilterBuilder filter) {
                            if (leftCondition.getOperator() == SQLBinaryOperator.BooleanAnd) {
                                subBoolFilter.must(filter);
                            }
                            if (leftCondition.getOperator() == SQLBinaryOperator.BooleanOr) {
                                subBoolFilter.should(filter);
                            }
                        }
                    });
                    curFilterList.add(subBoolFilter);
                }

                if (!rightCondition.isAndOr() || rightCondition.operator == binaryOperator) {
                    curFilterList.addAll(rightCondition.getFilterList());
                } else {
                    final BoolFilterBuilder subBoolFilter = FilterBuilders.boolFilter();
                    rightCondition.getFilterList().stream().forEach(new Consumer<FilterBuilder>() {
                        @Override
                        public void accept(FilterBuilder filter) {
                            if (rightCondition.getOperator() == SQLBinaryOperator.BooleanAnd) {
                                subBoolFilter.must(filter);
                            }
                            if (rightCondition.getOperator() == SQLBinaryOperator.BooleanOr) {
                                subBoolFilter.should(filter);
                            }
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
            final SQLBinaryOperator binaryOperator = sqlBinOpExpr.getOperator();

            if (ElasticSqlParseUtil.isValidBinOperator(binaryOperator)) {
                //EQ NEQ
                if (SQLBinaryOperator.Equality == binaryOperator || SQLBinaryOperator.LessThanOrGreater == binaryOperator || SQLBinaryOperator.NotEqual == binaryOperator) {
                    final Object targetVal = ElasticSqlParseUtil.transferSqlArg(sqlBinOpExpr.getRight(), dslContext.getSqlArgs());
                    return parseCondition(sqlBinOpExpr.getLeft(), dslContext.getParseResult().getQueryAs(), new ConditionFilterBuilder() {
                        @Override
                        public FilterBuilder buildFilter(String idfName) {
                            FilterBuilder eqFilter = FilterBuilders.termFilter(idfName, targetVal);
                            if (SQLBinaryOperator.Equality == binaryOperator) {
                                return eqFilter;
                            } else {
                                return FilterBuilders.notFilter(eqFilter);
                            }
                        }
                    });
                }

                //GT GTE LT LTE
                if (SQLBinaryOperator.GreaterThan == binaryOperator || SQLBinaryOperator.GreaterThanOrEqual == binaryOperator
                        || SQLBinaryOperator.LessThan == binaryOperator || SQLBinaryOperator.LessThanOrEqual == binaryOperator) {
                    final Object targetVal = ElasticSqlParseUtil.transferSqlArg(sqlBinOpExpr.getRight(), dslContext.getSqlArgs());
                    return parseCondition(sqlBinOpExpr.getLeft(), dslContext.getParseResult().getQueryAs(), new ConditionFilterBuilder() {
                        @Override
                        public FilterBuilder buildFilter(String idfName) {
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
                        }
                    });
                }

                //IS / IS NOT
                if (SQLBinaryOperator.Is == binaryOperator || SQLBinaryOperator.IsNot == binaryOperator) {
                    if (!(sqlBinOpExpr.getRight() instanceof SQLNullExpr)) {
                        throw new ElasticSql2DslException("[syntax error] Is/IsNot expr right part should be null");
                    }
                    return parseCondition(sqlBinOpExpr.getLeft(), dslContext.getParseResult().getQueryAs(), new ConditionFilterBuilder() {
                        @Override
                        public FilterBuilder buildFilter(String idfName) {
                            FilterBuilder missingFilter = FilterBuilders.missingFilter(idfName);
                            if (SQLBinaryOperator.IsNot == binaryOperator) {
                                return FilterBuilders.notFilter(missingFilter);
                            }
                            return missingFilter;
                        }
                    });
                }
            }
        } else if (sqlExpr instanceof SQLInListExpr) {
            final SQLInListExpr inListExpr = (SQLInListExpr) sqlExpr;
            if (CollectionUtils.isEmpty(inListExpr.getTargetList())) {
                throw new ElasticSql2DslException("[syntax error] In list expr target list cannot be blank");
            }
            final Object[] targetInList = ElasticSqlParseUtil.transferSqlArgs(inListExpr.getTargetList(), dslContext.getSqlArgs());
            return parseCondition(inListExpr.getExpr(), dslContext.getParseResult().getQueryAs(), new ConditionFilterBuilder() {
                @Override
                public FilterBuilder buildFilter(String idfName) {
                    if (inListExpr.isNot()) {
                        return FilterBuilders.notFilter(FilterBuilders.inFilter(idfName, targetInList));
                    } else {
                        return FilterBuilders.inFilter(idfName, targetInList);
                    }
                }
            });
        } else if (sqlExpr instanceof SQLBetweenExpr) {
            SQLBetweenExpr betweenExpr = (SQLBetweenExpr) sqlExpr;

            final Object from = ElasticSqlParseUtil.transferSqlArg(betweenExpr.getBeginExpr(), dslContext.getSqlArgs());
            final Object to = ElasticSqlParseUtil.transferSqlArg(betweenExpr.getEndExpr(), dslContext.getSqlArgs());

            if (from == null || to == null) {
                throw new ElasticSql2DslException("[syntax error] Between Expr only support one of [number,date] arg type");
            }

            return parseCondition(betweenExpr.getTestExpr(), dslContext.getParseResult().getQueryAs(), new ConditionFilterBuilder() {
                @Override
                public FilterBuilder buildFilter(String idfName) {
                    return FilterBuilders.rangeFilter(idfName).gte(from).lte(to);
                }
            });
        }
        throw new ElasticSql2DslException("[syntax error] Can not support syntax type: " + sqlExpr.toString());
    }

    private FilterBuilder parseCondition(SQLExpr sqlExpr, String queryAs, final ConditionFilterBuilder filterBuilder) {
        final List<FilterBuilder> tmpFilterList = Lists.newLinkedList();
        ElasticSqlIdentifierHelper.parseSqlIdentifier(sqlExpr, queryAs, new ElasticSqlIdentifierHelper.ElasticSqlSinglePropertyFunc() {
            @Override
            public void parse(String propertyName) {
                FilterBuilder originalFilter = filterBuilder.buildFilter(propertyName);
                tmpFilterList.add(originalFilter);
            }
        }, new ElasticSqlIdentifierHelper.ElasticSqlPathPropertyFunc() {
            @Override
            public void parse(String propertyPath, String propertyName) {
                FilterBuilder originalFilter = filterBuilder.buildFilter(propertyName);
                FilterBuilder nestFilter = FilterBuilders.nestedFilter(propertyPath, originalFilter);
                tmpFilterList.add(nestFilter);
            }
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
