package org.elasticsearch.dsl.parser;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.*;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.elasticsearch.dsl.ElasticDslContext;
import org.elasticsearch.dsl.ElasticSqlParseUtil;
import org.elasticsearch.dsl.SQLConditionOperator;
import org.elasticsearch.dsl.SQLIdentifierType;
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
        BoolFilterBuilder whereCondition = FilterBuilders.boolFilter();
        if (queryBlock.getWhere() != null) {
            SqlCondition sqlCondition = parseFilterCondition(dslContext, queryBlock.getWhere());
            if (!sqlCondition.isAndOr()) {
                whereCondition.must(sqlCondition.getFilterList().get(0));
            } else {
                for (FilterBuilder filter : sqlCondition.getFilterList()) {
                    if (sqlCondition.getOperator() == SQLBinaryOperator.BooleanAnd) {
                        whereCondition.must(filter);
                    }
                    if (sqlCondition.getOperator() == SQLBinaryOperator.BooleanOr) {
                        whereCondition.should(filter);
                    }
                }
            }
        }
        dslContext.getParseResult().setWhereCondition(whereCondition);
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
                    Object targetVal = ElasticSqlParseUtil.transferSqlArg(sqlBinOpExpr.getRight(), dslContext.getSqlArgs());

                    SQLConditionOperator operator = SQLBinaryOperator.Equality == binaryOperator ? SQLConditionOperator.Equality : SQLConditionOperator.NotEqual;
                    return parseCondition(sqlBinOpExpr.getLeft(), operator, new Object[]{targetVal}, dslContext.getParseResult().getQueryAs(), new ConditionFilterBuilder() {
                        @Override
                        public FilterBuilder buildFilter(String leftIdfName, SQLConditionOperator operator, Object[] rightParamValues) {
                            FilterBuilder eqFilter = FilterBuilders.termFilter(leftIdfName, rightParamValues[0]);
                            if (SQLConditionOperator.Equality == operator) {
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

                    SQLConditionOperator operator = null;
                    if (SQLBinaryOperator.GreaterThan == binaryOperator) {
                        operator = SQLConditionOperator.GreaterThan;
                    } else if (SQLBinaryOperator.GreaterThanOrEqual == binaryOperator) {
                        operator = SQLConditionOperator.GreaterThanOrEqual;
                    } else if (SQLBinaryOperator.LessThan == binaryOperator) {
                        operator = SQLConditionOperator.LessThan;
                    } else if (SQLBinaryOperator.LessThanOrEqual == binaryOperator) {
                        operator = SQLConditionOperator.LessThanOrEqual;
                    }

                    Object targetVal = ElasticSqlParseUtil.transferSqlArg(sqlBinOpExpr.getRight(), dslContext.getSqlArgs());
                    return parseCondition(sqlBinOpExpr.getLeft(), operator, new Object[]{targetVal}, dslContext.getParseResult().getQueryAs(), new ConditionFilterBuilder() {
                        @Override
                        public FilterBuilder buildFilter(String leftIdfName, SQLConditionOperator operator, Object[] rightParamValues) {
                            FilterBuilder rangeFilter = null;
                            if (SQLConditionOperator.GreaterThan == operator) {
                                rangeFilter = FilterBuilders.rangeFilter(leftIdfName).gt(rightParamValues[0]);
                            } else if (SQLConditionOperator.GreaterThanOrEqual == operator) {
                                rangeFilter = FilterBuilders.rangeFilter(leftIdfName).gte(rightParamValues[0]);
                            } else if (SQLConditionOperator.LessThan == operator) {
                                rangeFilter = FilterBuilders.rangeFilter(leftIdfName).lt(rightParamValues[0]);
                            } else if (SQLConditionOperator.LessThanOrEqual == operator) {
                                rangeFilter = FilterBuilders.rangeFilter(leftIdfName).lte(rightParamValues[0]);
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
                    SQLConditionOperator operator = SQLBinaryOperator.Is == binaryOperator ? SQLConditionOperator.IsNull : SQLConditionOperator.IsNotNull;
                    return parseCondition(sqlBinOpExpr.getLeft(), operator, null, dslContext.getParseResult().getQueryAs(), new ConditionFilterBuilder() {
                        @Override
                        public FilterBuilder buildFilter(String leftIdfName, SQLConditionOperator operator, Object[] rightParamValues) {
                            FilterBuilder missingFilter = FilterBuilders.missingFilter(leftIdfName);
                            if (SQLConditionOperator.IsNotNull == operator) {
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
            Object[] targetInList = ElasticSqlParseUtil.transferSqlArgs(inListExpr.getTargetList(), dslContext.getSqlArgs());
            SQLConditionOperator operator = inListExpr.isNot() ? SQLConditionOperator.NotIn : SQLConditionOperator.In;
            return parseCondition(inListExpr.getExpr(), operator, targetInList, dslContext.getParseResult().getQueryAs(), new ConditionFilterBuilder() {
                @Override
                public FilterBuilder buildFilter(String leftIdfName, SQLConditionOperator operator, Object[] rightParamValues) {
                    if (SQLConditionOperator.NotIn == operator) {
                        return FilterBuilders.notFilter(FilterBuilders.inFilter(leftIdfName, rightParamValues));
                    } else {
                        return FilterBuilders.inFilter(leftIdfName, rightParamValues);
                    }
                }
            });
        } else if (sqlExpr instanceof SQLBetweenExpr) {
            SQLBetweenExpr betweenExpr = (SQLBetweenExpr) sqlExpr;

            Object from = ElasticSqlParseUtil.transferSqlArg(betweenExpr.getBeginExpr(), dslContext.getSqlArgs());
            Object to = ElasticSqlParseUtil.transferSqlArg(betweenExpr.getEndExpr(), dslContext.getSqlArgs());

            if (from == null || to == null) {
                throw new ElasticSql2DslException("[syntax error] Between Expr only support one of [number,date] arg type");
            }

            return parseCondition(betweenExpr.getTestExpr(), SQLConditionOperator.BetweenAnd, new Object[]{from, to}, dslContext.getParseResult().getQueryAs(), new ConditionFilterBuilder() {
                @Override
                public FilterBuilder buildFilter(String leftIdfName, SQLConditionOperator operator, Object[] rightParamValues) {
                    return FilterBuilders.rangeFilter(leftIdfName).gte(rightParamValues[0]).lte(rightParamValues[1]);
                }
            });
        }
        throw new ElasticSql2DslException("[syntax error] Can not support syntax type: " + sqlExpr.toString());
    }

    private FilterBuilder parseCondition(SQLExpr leftIdentifierExpr, SQLConditionOperator operator, Object[] rightParamValues, String queryAs, final ConditionFilterBuilder filterBuilder) {
        final List<FilterBuilder> conditionCollector = Lists.newLinkedList();
        final Object[] pRightParamValues = rightParamValues;
        final SQLConditionOperator pOperator = operator;
        SQLIdentifierType idfType = ElasticSqlIdentifierHelper.parseSqlIdentifier(leftIdentifierExpr, queryAs, new ElasticSqlIdentifierHelper.ElasticSqlSinglePropertyFunc() {
            @Override
            public void parse(String propertyName) {
                FilterBuilder originalFilter = filterBuilder.buildFilter(propertyName, pOperator, pRightParamValues);
                conditionCollector.add(originalFilter);
            }
        }, new ElasticSqlIdentifierHelper.ElasticSqlPathPropertyFunc() {
            @Override
            public void parse(String propertyPath, String propertyName) {
                FilterBuilder originalFilter = filterBuilder.buildFilter(propertyName, pOperator, pRightParamValues);
                FilterBuilder nestFilter = FilterBuilders.nestedFilter(propertyPath, originalFilter);
                conditionCollector.add(nestFilter);
            }
        });
        if (CollectionUtils.isNotEmpty(conditionCollector)) {
            //todo invoke listener
            onAtomConditionParse("", rightParamValues, idfType, operator);
            return conditionCollector.get(0);
        }
        return null;
    }

    public void onAtomConditionParse(String paramName, Object[] paramValues, SQLIdentifierType paramPropertyType, SQLConditionOperator operator) {

    }

    private abstract class ConditionFilterBuilder {
        public abstract FilterBuilder buildFilter(String leftIdfName, SQLConditionOperator operator, Object[] rightParamValues);
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
