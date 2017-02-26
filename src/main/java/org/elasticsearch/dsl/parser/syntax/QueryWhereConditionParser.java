package org.elasticsearch.dsl.parser.syntax;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.*;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.elasticsearch.dsl.bean.ElasticDslContext;
import org.elasticsearch.dsl.bean.ElasticSqlQueryField;
import org.elasticsearch.dsl.bean.SqlCondition;
import org.elasticsearch.dsl.enums.SQLConditionOperator;
import org.elasticsearch.dsl.exception.ElasticSql2DslException;
import org.elasticsearch.dsl.parser.QueryParser;
import org.elasticsearch.dsl.parser.helper.ElasticSqlIdentifierHelper;
import org.elasticsearch.dsl.parser.listener.ParseActionListener;
import org.elasticsearch.dsl.parser.helper.ElasticSqlArgTransferHelper;
import org.elasticsearch.index.query.BoolFilterBuilder;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.sql.ElasticSqlSelectQueryBlock;

import java.util.List;
import java.util.function.Consumer;

public class QueryWhereConditionParser implements QueryParser {

    private ParseActionListener parseActionListener;

    public QueryWhereConditionParser(ParseActionListener parseActionListener) {
        this.parseActionListener = parseActionListener;
    }

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

                if (!leftCondition.isAndOr() || leftCondition.getOperator() == binaryOperator) {
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

                if (!rightCondition.isAndOr() || rightCondition.getOperator() == binaryOperator) {
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

            if (isValidBinOperator(binaryOperator)) {
                //EQ NEQ
                if (SQLBinaryOperator.Equality == binaryOperator || SQLBinaryOperator.LessThanOrGreater == binaryOperator || SQLBinaryOperator.NotEqual == binaryOperator) {
                    Object targetVal = ElasticSqlArgTransferHelper.transferSqlArg(sqlBinOpExpr.getRight(), dslContext.getSqlArgs());

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

                    Object targetVal = ElasticSqlArgTransferHelper.transferSqlArg(sqlBinOpExpr.getRight(), dslContext.getSqlArgs());
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
            Object[] targetInList = ElasticSqlArgTransferHelper.transferSqlArgs(inListExpr.getTargetList(), dslContext.getSqlArgs());
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

            Object from = ElasticSqlArgTransferHelper.transferSqlArg(betweenExpr.getBeginExpr(), dslContext.getSqlArgs());
            Object to = ElasticSqlArgTransferHelper.transferSqlArg(betweenExpr.getEndExpr(), dslContext.getSqlArgs());

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
        ElasticSqlQueryField sqlIdentifier = ElasticSqlIdentifierHelper.parseSqlIdentifier(leftIdentifierExpr, queryAs, new ElasticSqlIdentifierHelper.SQLFlatFieldFunc() {
            @Override
            public void parse(String flatFieldName) {
                FilterBuilder originalFilter = filterBuilder.buildFilter(flatFieldName, pOperator, pRightParamValues);
                conditionCollector.add(originalFilter);
            }
        }, new ElasticSqlIdentifierHelper.SQLNestedFieldFunc() {
            @Override
            public void parse(String nestedDocPath, String fieldName) {
                FilterBuilder originalFilter = filterBuilder.buildFilter(fieldName, pOperator, pRightParamValues);
                FilterBuilder nestFilter = FilterBuilders.nestedFilter(nestedDocPath, originalFilter);
                conditionCollector.add(nestFilter);
            }
        });
        if (CollectionUtils.isNotEmpty(conditionCollector)) {
            onAtomConditionParse(sqlIdentifier, rightParamValues, operator);
            return conditionCollector.get(0);
        }
        return null;
    }

    private void onAtomConditionParse(ElasticSqlQueryField paramName, Object[] paramValues, SQLConditionOperator operator) {
        try {
            parseActionListener.onAtomConditionParse(paramName, paramValues, operator);
        } catch (Exception ex) {
            try {
                parseActionListener.onFailure(ex);
            } catch (Exception exp) {
                //ignore;
            }
        }
    }

    @FunctionalInterface
    private interface ConditionFilterBuilder {
        FilterBuilder buildFilter(String leftIdfName, SQLConditionOperator operator, Object[] rightParamValues);
    }

    private boolean isValidBinOperator(SQLBinaryOperator binaryOperator) {
        return binaryOperator == SQLBinaryOperator.Equality
                || binaryOperator == SQLBinaryOperator.NotEqual
                || binaryOperator == SQLBinaryOperator.LessThanOrGreater
                || binaryOperator == SQLBinaryOperator.GreaterThan
                || binaryOperator == SQLBinaryOperator.GreaterThanOrEqual
                || binaryOperator == SQLBinaryOperator.LessThan
                || binaryOperator == SQLBinaryOperator.LessThanOrEqual
                || binaryOperator == SQLBinaryOperator.Is
                || binaryOperator == SQLBinaryOperator.IsNot;
    }
}
