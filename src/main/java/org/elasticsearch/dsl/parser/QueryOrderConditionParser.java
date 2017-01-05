package org.elasticsearch.dsl.parser;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLOrderBy;
import com.alibaba.druid.sql.ast.SQLOrderingSpecification;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.statement.SQLSelectOrderByItem;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.elasticsearch.dsl.ElasticDslContext;
import org.elasticsearch.dsl.ElasticSqlParseUtil;
import org.elasticsearch.dsl.exception.ElasticSql2DslException;
import org.elasticsearch.dsl.parser.helper.ElasticSqlIdentifierHelper;
import org.elasticsearch.dsl.parser.helper.ElasticSqlMethodInvokeHelper;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.sql.ElasticSqlSelectQueryBlock;

import java.util.List;

public class QueryOrderConditionParser implements QueryParser {
    @Override
    public void parse(ElasticDslContext dslContext) {
        ElasticSqlSelectQueryBlock queryBlock = (ElasticSqlSelectQueryBlock) dslContext.getQueryExpr().getSubQuery().getQuery();
        SQLOrderBy sqlOrderBy = queryBlock.getOrderBy();
        if (sqlOrderBy != null && CollectionUtils.isNotEmpty(sqlOrderBy.getItems())) {
            for (SQLSelectOrderByItem orderByItem : sqlOrderBy.getItems()) {
                SortBuilder orderBy = parseOrderCondition(orderByItem, dslContext.getParseResult().getQueryAs(), dslContext.getSqlArgs());
                if (orderBy != null) {
                    dslContext.getParseResult().addSort(orderBy);
                }
            }
        }
    }

    private SortBuilder parseOrderCondition(final SQLSelectOrderByItem orderByItem, String queryAs, Object[] sqlArgs) {
        if (orderByItem.getExpr() instanceof SQLPropertyExpr || orderByItem.getExpr() instanceof SQLIdentifierExpr) {
            return parseCondition(orderByItem.getExpr(), queryAs, new ConditionSortBuilder() {
                @Override
                public FieldSortBuilder buildSort(String idfName) {
                    if (SQLOrderingSpecification.ASC == orderByItem.getType()) {
                        return SortBuilders.fieldSort(idfName).order(SortOrder.ASC);
                    } else {
                        return SortBuilders.fieldSort(idfName).order(SortOrder.DESC);
                    }
                }
            });
        }
        if (orderByItem.getExpr() instanceof SQLMethodInvokeExpr) {
            final SQLMethodInvokeExpr methodInvokeExpr = (SQLMethodInvokeExpr) orderByItem.getExpr();
            //nvl method
            if (ElasticSqlMethodInvokeHelper.NVL_METHOD.equalsIgnoreCase(methodInvokeExpr.getMethodName())) {
                ElasticSqlMethodInvokeHelper.checkNvlMethod(methodInvokeExpr);
                final Object valueArg = ElasticSqlParseUtil.transferSqlArg(methodInvokeExpr.getParameters().get(1), sqlArgs);
                return parseCondition(methodInvokeExpr.getParameters().get(0), queryAs, new ConditionSortBuilder() {
                    @Override
                    public FieldSortBuilder buildSort(String idfName) {
                        FieldSortBuilder fieldSortBuilder = null;
                        if (SQLOrderingSpecification.ASC == orderByItem.getType()) {
                            fieldSortBuilder = SortBuilders.fieldSort(idfName).order(SortOrder.ASC).missing(valueArg);
                        } else {
                            fieldSortBuilder = SortBuilders.fieldSort(idfName).order(SortOrder.DESC).missing(valueArg);
                        }
                        if (methodInvokeExpr.getParameters().size() == 3) {
                            SQLExpr sortModArg = methodInvokeExpr.getParameters().get(2);
                            String sortModeText = ((SQLCharExpr) sortModArg).getText();
                            fieldSortBuilder.sortMode(SortOption.get(sortModeText).mode());
                        }
                        return fieldSortBuilder;
                    }
                });
            }

            //nested或者inner doc类型
            if (ElasticSqlMethodInvokeHelper.NESTED_DOC_METHOD.equalsIgnoreCase(methodInvokeExpr.getMethodName())
                    || ElasticSqlMethodInvokeHelper.INNER_DOC_METHOD.equalsIgnoreCase(methodInvokeExpr.getMethodName())) {
                return parseCondition(methodInvokeExpr, queryAs, new ConditionSortBuilder() {
                    @Override
                    public FieldSortBuilder buildSort(String idfName) {
                        return SortBuilders.fieldSort(idfName).order(
                                SQLOrderingSpecification.ASC == orderByItem.getType() ? SortOrder.ASC : SortOrder.DESC
                        );
                    }
                });
            }
        }
        throw new ElasticSql2DslException("[syntax error] ElasticSql cannot support sort type: " + orderByItem.getExpr().getClass());
    }

    private SortBuilder parseCondition(SQLExpr sqlExpr, String queryAs, final ConditionSortBuilder sortBuilder) {
        final List<SortBuilder> tmpSortList = Lists.newLinkedList();
        ElasticSqlIdentifierHelper.parseSqlIdentifier(sqlExpr, queryAs, new ElasticSqlIdentifierHelper.ElasticSqlSinglePropertyFunc() {
            @Override
            public void parse(String propertyName) {
                FieldSortBuilder originalSort = sortBuilder.buildSort(propertyName);
                tmpSortList.add(originalSort);
            }
        }, new ElasticSqlIdentifierHelper.ElasticSqlPathPropertyFunc() {
            @Override
            public void parse(String propertyPath, String propertyName) {
                FieldSortBuilder originalSort = sortBuilder.buildSort(propertyName);
                originalSort.setNestedPath(propertyPath);
                tmpSortList.add(originalSort);
            }
        });
        if (CollectionUtils.isNotEmpty(tmpSortList)) {
            return tmpSortList.get(0);
        }
        return null;
    }

    @FunctionalInterface
    private interface ConditionSortBuilder {
        FieldSortBuilder buildSort(String idfName);
    }

    public enum SortOption {
        SUM {
            @Override
            public String mode() {
                return "sum";
            }
        },
        MIN {
            @Override
            public String mode() {
                return "min";
            }
        },
        MAX {
            @Override
            public String mode() {
                return "max";
            }
        },
        AVG {
            @Override
            public String mode() {
                return "avg";
            }
        };

        public abstract String mode();

        @Override
        public String toString() {
            return mode();
        }

        public static SortOption get(String mode) {
            SortOption op = null;
            for (SortOption option : SortOption.values()) {
                if (option.mode().equalsIgnoreCase(mode)) {
                    op = option;
                }
            }
            return op;
        }
    }
}
