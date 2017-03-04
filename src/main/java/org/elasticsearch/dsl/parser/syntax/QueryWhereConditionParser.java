package org.elasticsearch.dsl.parser.syntax;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.*;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.elasticsearch.dsl.bean.AtomFilter;
import org.elasticsearch.dsl.bean.ElasticDslContext;
import org.elasticsearch.dsl.bean.ElasticSqlQueryField;
import org.elasticsearch.dsl.bean.SQLCondition;
import org.elasticsearch.dsl.enums.QueryFieldType;
import org.elasticsearch.dsl.enums.SQLBoolOperator;
import org.elasticsearch.dsl.enums.SQLConditionOperator;
import org.elasticsearch.dsl.enums.SQLConditionType;
import org.elasticsearch.dsl.exception.ElasticSql2DslException;
import org.elasticsearch.dsl.parser.QueryParser;
import org.elasticsearch.dsl.parser.helper.ElasticSqlArgTransferHelper;
import org.elasticsearch.dsl.parser.listener.ParseActionListener;
import org.elasticsearch.index.query.BoolFilterBuilder;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.sql.ElasticSqlSelectQueryBlock;

import java.util.List;

public class QueryWhereConditionParser implements QueryParser {

    private ParseActionListener parseActionListener;

    public QueryWhereConditionParser(ParseActionListener parseActionListener) {
        this.parseActionListener = parseActionListener;
    }

    @Override
    public void parse(ElasticDslContext dslContext) {
        ElasticSqlSelectQueryBlock queryBlock = (ElasticSqlSelectQueryBlock) dslContext.getQueryExpr().getSubQuery().getQuery();

        if (queryBlock.getWhere() != null) {
            SQLCondition whereCondition = parseFilterCondition(dslContext, queryBlock.getWhere());

            SQLBoolOperator operator = whereCondition.getOperator();
            if(SQLConditionType.Atom == whereCondition.getSQLConditionType()) {
                operator = SQLBoolOperator.AND;
            }

            BoolFilterBuilder boolFilter = mergeAtomFilter(whereCondition.getFilterList(), operator);
            dslContext.getParseResult().setWhereCondition(boolFilter);
        }
    }

    private SQLCondition parseFilterCondition(ElasticDslContext dslContext, SQLExpr sqlExpr) {
        if (sqlExpr instanceof SQLBinaryOpExpr) {
            SQLBinaryOpExpr sqlBinOpExpr = (SQLBinaryOpExpr) sqlExpr;
            SQLBinaryOperator binaryOperator = sqlBinOpExpr.getOperator();
            if (SQLBinaryOperator.BooleanAnd == binaryOperator || SQLBinaryOperator.BooleanOr == binaryOperator) {

                SQLBoolOperator operator = SQLBinaryOperator.BooleanAnd == binaryOperator ? SQLBoolOperator.AND : SQLBoolOperator.OR;

                SQLCondition leftCondition = parseFilterCondition(dslContext, sqlBinOpExpr.getLeft());
                SQLCondition rightCondition = parseFilterCondition(dslContext, sqlBinOpExpr.getRight());

                List<AtomFilter> curFilterList = Lists.newArrayList();
                combineFilterBuilder(curFilterList, leftCondition, operator);
                combineFilterBuilder(curFilterList, rightCondition, operator);


                return new SQLCondition(curFilterList, operator);
            }
        }
        return new SQLCondition(parseAtomFilterCondition(dslContext, sqlExpr), SQLConditionType.Atom);
    }

    private void combineFilterBuilder(List<AtomFilter> combiner, SQLCondition sqlCondition, SQLBoolOperator binOperator) {
        if (SQLConditionType.Atom == sqlCondition.getSQLConditionType() || sqlCondition.getOperator() == binOperator) {
            combiner.addAll(sqlCondition.getFilterList());
        }
        else {
            //todo binOperator -> sqlCondition.getOperator()
            BoolFilterBuilder boolFilter = mergeAtomFilter(sqlCondition.getFilterList(), sqlCondition.getOperator());
            combiner.add(new AtomFilter(boolFilter));
        }
    }

    private BoolFilterBuilder mergeAtomFilter(List<AtomFilter> atomFilterList, SQLBoolOperator operator) {
        BoolFilterBuilder subBoolFilter = FilterBuilders.boolFilter();
        ListMultimap<String, FilterBuilder> listMultiMap = ArrayListMultimap.create();

        for (AtomFilter atomFilter : atomFilterList) {
            if(Boolean.FALSE == atomFilter.getNestedFilter()) {
                if (operator == SQLBoolOperator.AND) {
                    subBoolFilter.must(atomFilter.getFilter());
                }
                if (operator == SQLBoolOperator.OR) {
                    subBoolFilter.should(atomFilter.getFilter());
                }
            }
            else {
                String nestedDocPrefix = atomFilter.getNestedFilterPathContext();
                listMultiMap.put(nestedDocPrefix, atomFilter.getFilter());
            }
        }

        for (String nestedDocPrefix : listMultiMap.keySet()) {
            List<FilterBuilder> nestedFilterList = listMultiMap.get(nestedDocPrefix);

            if(nestedFilterList.size() == 1) {
                if (operator == SQLBoolOperator.AND) {
                    subBoolFilter.must(FilterBuilders.nestedFilter(nestedDocPrefix, nestedFilterList.get(0)));
                }
                if (operator == SQLBoolOperator.OR) {
                    subBoolFilter.should(FilterBuilders.nestedFilter(nestedDocPrefix, nestedFilterList.get(0)));
                }
                continue;
            }

            BoolFilterBuilder boolNestedFilter = FilterBuilders.boolFilter();
            for (FilterBuilder nestedFilterItem : nestedFilterList) {
                if (operator == SQLBoolOperator.AND) {
                    boolNestedFilter.must(nestedFilterItem);
                }
                if (operator == SQLBoolOperator.OR) {
                    boolNestedFilter.should(nestedFilterItem);
                }
            }

            if (operator == SQLBoolOperator.AND) {
                subBoolFilter.must(FilterBuilders.nestedFilter(nestedDocPrefix, boolNestedFilter));
            }
            if (operator == SQLBoolOperator.OR) {
                subBoolFilter.should(FilterBuilders.nestedFilter(nestedDocPrefix, boolNestedFilter));
            }

        }
        return subBoolFilter;
    }

    private AtomFilter parseAtomFilterCondition(ElasticDslContext dslContext, SQLExpr sqlExpr) {
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
            SQLInListExpr inListExpr = (SQLInListExpr) sqlExpr;
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

    private AtomFilter parseCondition(SQLExpr leftQueryFieldExpr, SQLConditionOperator operator, Object[] rightParamValues, String queryAs, ConditionFilterBuilder filterBuilder) {
        QueryFieldParser queryFieldParser = new QueryFieldParser();
        ElasticSqlQueryField queryField = queryFieldParser.parseConditionQueryField(leftQueryFieldExpr, queryAs);

        AtomFilter atomFilter = null;
        if (queryField.getQueryFieldType() == QueryFieldType.RootDocField || queryField.getQueryFieldType() == QueryFieldType.InnerDocField) {
            FilterBuilder originalFilter = filterBuilder.buildFilter(queryField.getQueryFieldFullName(), operator, rightParamValues);
            atomFilter = new AtomFilter(originalFilter);
        }

        if (queryField.getQueryFieldType() == QueryFieldType.NestedDocField) {
            FilterBuilder originalFilter = filterBuilder.buildFilter(queryField.getQueryFieldFullName(), operator, rightParamValues);
            atomFilter = new AtomFilter(originalFilter, queryField.getNestedDocContextPath());
        }

        if (atomFilter == null) {
            throw new ElasticSql2DslException(String.format("[syntax error] where condition field can not support type[%s]", queryField.getQueryFieldType()));
        }

        onAtomConditionParse(queryField, rightParamValues, operator);

        return atomFilter;
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

    public boolean isValidBinOperator(SQLBinaryOperator binaryOperator) {
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
