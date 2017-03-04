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
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
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

            BoolQueryBuilder boolQuery = mergeAtomFilter(whereCondition.getFilterList(), operator);
            dslContext.getParseResult().setWhereCondition(boolQuery);
        }
    }

    protected SQLCondition parseFilterCondition(ElasticDslContext dslContext, SQLExpr sqlExpr) {
        if (sqlExpr instanceof SQLBinaryOpExpr) {
            SQLBinaryOpExpr sqlBinOpExpr = (SQLBinaryOpExpr) sqlExpr;
            SQLBinaryOperator binaryOperator = sqlBinOpExpr.getOperator();
            if (SQLBinaryOperator.BooleanAnd == binaryOperator || SQLBinaryOperator.BooleanOr == binaryOperator) {

                SQLBoolOperator operator = SQLBinaryOperator.BooleanAnd == binaryOperator ? SQLBoolOperator.AND : SQLBoolOperator.OR;

                SQLCondition leftCondition = parseFilterCondition(dslContext, sqlBinOpExpr.getLeft());
                SQLCondition rightCondition = parseFilterCondition(dslContext, sqlBinOpExpr.getRight());

                List<AtomFilter> curFilterList = Lists.newArrayList();
                combineQueryBuilder(curFilterList, leftCondition, operator);
                combineQueryBuilder(curFilterList, rightCondition, operator);


                return new SQLCondition(curFilterList, operator);
            }
        }
        return new SQLCondition(parseAtomFilterCondition(dslContext, sqlExpr), SQLConditionType.Atom);
    }

    private void combineQueryBuilder(List<AtomFilter> combiner, SQLCondition sqlCondition, SQLBoolOperator binOperator) {
        if (SQLConditionType.Atom == sqlCondition.getSQLConditionType() || sqlCondition.getOperator() == binOperator) {
            combiner.addAll(sqlCondition.getFilterList());
        }
        else {
            //todo binOperator -> sqlCondition.getOperator()
            BoolQueryBuilder boolQuery = mergeAtomFilter(sqlCondition.getFilterList(), sqlCondition.getOperator());
            combiner.add(new AtomFilter(boolQuery));
        }
    }

    protected BoolQueryBuilder mergeAtomFilter(List<AtomFilter> atomFilterList, SQLBoolOperator operator) {
        BoolQueryBuilder subboolQuery = QueryBuilders.boolQuery();
        ListMultimap<String, QueryBuilder> listMultiMap = ArrayListMultimap.create();

        for (AtomFilter atomFilter : atomFilterList) {
            if(Boolean.FALSE == atomFilter.getNestedFilter()) {
                if (operator == SQLBoolOperator.AND) {
                    subboolQuery.must(atomFilter.getFilter());
                }
                if (operator == SQLBoolOperator.OR) {
                    subboolQuery.should(atomFilter.getFilter());
                }
            }
            else {
                String nestedDocPrefix = atomFilter.getNestedFilterPathContext();
                listMultiMap.put(nestedDocPrefix, atomFilter.getFilter());
            }
        }

        for (String nestedDocPrefix : listMultiMap.keySet()) {
            List<QueryBuilder> nestedQueryList = listMultiMap.get(nestedDocPrefix);

            if(nestedQueryList.size() == 1) {
                if (operator == SQLBoolOperator.AND) {
                    subboolQuery.must(QueryBuilders.nestedQuery(nestedDocPrefix, nestedQueryList.get(0)));
                }
                if (operator == SQLBoolOperator.OR) {
                    subboolQuery.should(QueryBuilders.nestedQuery(nestedDocPrefix, nestedQueryList.get(0)));
                }
                continue;
            }

            BoolQueryBuilder boolNestedFilter = QueryBuilders.boolQuery();
            for (QueryBuilder nestedQueryItem : nestedQueryList) {
                if (operator == SQLBoolOperator.AND) {
                    boolNestedFilter.must(nestedQueryItem);
                }
                if (operator == SQLBoolOperator.OR) {
                    boolNestedFilter.should(nestedQueryItem);
                }
            }

            if (operator == SQLBoolOperator.AND) {
                subboolQuery.must(QueryBuilders.nestedQuery(nestedDocPrefix, boolNestedFilter));
            }
            if (operator == SQLBoolOperator.OR) {
                subboolQuery.should(QueryBuilders.nestedQuery(nestedDocPrefix, boolNestedFilter));
            }

        }
        return subboolQuery;
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
                    return parseCondition(sqlBinOpExpr.getLeft(), operator, new Object[]{targetVal}, dslContext.getParseResult().getQueryAs(), new ConditionQueryBuilder() {
                        @Override
                        public QueryBuilder buildFilter(String leftIdfName, SQLConditionOperator operator, Object[] rightParamValues) {
                            QueryBuilder eqFilter = QueryBuilders.termQuery(leftIdfName, rightParamValues[0]);
                            if (SQLConditionOperator.Equality == operator) {
                                return eqFilter;
                            } else {
                                return QueryBuilders.boolQuery().mustNot(eqFilter);
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
                    return parseCondition(sqlBinOpExpr.getLeft(), operator, new Object[]{targetVal}, dslContext.getParseResult().getQueryAs(), new ConditionQueryBuilder() {
                        @Override
                        public QueryBuilder buildFilter(String leftIdfName, SQLConditionOperator operator, Object[] rightParamValues) {
                            QueryBuilder rangeQuery = null;
                            if (SQLConditionOperator.GreaterThan == operator) {
                                rangeQuery = QueryBuilders.rangeQuery(leftIdfName).gt(rightParamValues[0]);
                            } else if (SQLConditionOperator.GreaterThanOrEqual == operator) {
                                rangeQuery = QueryBuilders.rangeQuery(leftIdfName).gte(rightParamValues[0]);
                            } else if (SQLConditionOperator.LessThan == operator) {
                                rangeQuery = QueryBuilders.rangeQuery(leftIdfName).lt(rightParamValues[0]);
                            } else if (SQLConditionOperator.LessThanOrEqual == operator) {
                                rangeQuery = QueryBuilders.rangeQuery(leftIdfName).lte(rightParamValues[0]);
                            }
                            return rangeQuery;
                        }
                    });
                }

                //IS / IS NOT
                if (SQLBinaryOperator.Is == binaryOperator || SQLBinaryOperator.IsNot == binaryOperator) {
                    if (!(sqlBinOpExpr.getRight() instanceof SQLNullExpr)) {
                        throw new ElasticSql2DslException("[syntax error] Is/IsNot expr right part should be null");
                    }
                    SQLConditionOperator operator = SQLBinaryOperator.Is == binaryOperator ? SQLConditionOperator.IsNull : SQLConditionOperator.IsNotNull;
                    return parseCondition(sqlBinOpExpr.getLeft(), operator, null, dslContext.getParseResult().getQueryAs(), new ConditionQueryBuilder() {
                        @Override
                        public QueryBuilder buildFilter(String leftIdfName, SQLConditionOperator operator, Object[] rightParamValues) {
                            QueryBuilder missingFilter = QueryBuilders.missingQuery(leftIdfName);
                            if (SQLConditionOperator.IsNotNull == operator) {
                                return QueryBuilders.boolQuery().mustNot(missingFilter);
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
            return parseCondition(inListExpr.getExpr(), operator, targetInList, dslContext.getParseResult().getQueryAs(), new ConditionQueryBuilder() {
                @Override
                public QueryBuilder buildFilter(String leftIdfName, SQLConditionOperator operator, Object[] rightParamValues) {
                    if (SQLConditionOperator.NotIn == operator) {
                        return QueryBuilders.boolQuery().mustNot(QueryBuilders.termsQuery(leftIdfName, rightParamValues));
                    } else {
                        return QueryBuilders.termsQuery(leftIdfName, rightParamValues);
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

            return parseCondition(betweenExpr.getTestExpr(), SQLConditionOperator.BetweenAnd, new Object[]{from, to}, dslContext.getParseResult().getQueryAs(), new ConditionQueryBuilder() {
                @Override
                public QueryBuilder buildFilter(String leftIdfName, SQLConditionOperator operator, Object[] rightParamValues) {
                    return QueryBuilders.rangeQuery(leftIdfName).gte(rightParamValues[0]).lte(rightParamValues[1]);
                }
            });
        }
        throw new ElasticSql2DslException("[syntax error] Can not support syntax type: " + sqlExpr.toString());
    }

    private AtomFilter parseCondition(SQLExpr leftQueryFieldExpr, SQLConditionOperator operator, Object[] rightParamValues, String queryAs, ConditionQueryBuilder filterBuilder) {
        QueryFieldParser queryFieldParser = new QueryFieldParser();
        ElasticSqlQueryField queryField = queryFieldParser.parseConditionQueryField(leftQueryFieldExpr, queryAs);

        AtomFilter atomFilter = null;
        if (queryField.getQueryFieldType() == QueryFieldType.RootDocField || queryField.getQueryFieldType() == QueryFieldType.InnerDocField) {
            QueryBuilder originalFilter = filterBuilder.buildFilter(queryField.getQueryFieldFullName(), operator, rightParamValues);
            atomFilter = new AtomFilter(originalFilter);
        }

        if (queryField.getQueryFieldType() == QueryFieldType.NestedDocField) {
            QueryBuilder originalFilter = filterBuilder.buildFilter(queryField.getQueryFieldFullName(), operator, rightParamValues);
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
    private interface ConditionQueryBuilder {
        QueryBuilder buildFilter(String leftIdfName, SQLConditionOperator operator, Object[] rightParamValues);
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
