package org.elasticsearch.dsl.parser;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLOrderBy;
import com.alibaba.druid.sql.ast.SQLOrderingSpecification;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.SQLSelectOrderByItem;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.elasticsearch.dsl.ElasticDslContext;
import org.elasticsearch.dsl.exception.ElasticSql2DslException;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.sql.ElasticSqlSelectQueryBlock;

import java.util.List;

public class QueryOrderConditionParser implements ElasticSqlParser {
    @Override
    public void parse(ElasticDslContext dslContext) {
        ElasticSqlSelectQueryBlock queryBlock = (ElasticSqlSelectQueryBlock) dslContext.getQueryExpr().getSubQuery().getQuery();
        SQLOrderBy sqlOrderBy = queryBlock.getOrderBy();
        if (sqlOrderBy != null && CollectionUtils.isNotEmpty(sqlOrderBy.getItems())) {
            sqlOrderBy.getItems().stream().forEach(orderByItem -> {
                SortBuilder orderBy = parseOrderCondition(orderByItem, dslContext.getQueryAs());
                if (orderBy != null) {
                    dslContext.addSort(orderBy);
                }
            });
        }
    }

    private SortBuilder parseOrderCondition(SQLSelectOrderByItem orderByItem, String queryAs) {
        if (orderByItem.getExpr() instanceof SQLPropertyExpr || orderByItem.getExpr() instanceof SQLIdentifierExpr) {
            return parseCondition(orderByItem.getExpr(), queryAs, idfName -> {
                if (SQLOrderingSpecification.ASC == orderByItem.getType()) {
                    return SortBuilders.fieldSort(idfName).order(SortOrder.ASC);
                } else {
                    return SortBuilders.fieldSort(idfName).order(SortOrder.DESC);
                }
            });
        }
        if (orderByItem.getExpr() instanceof SQLMethodInvokeExpr) {
            SQLMethodInvokeExpr methodInvokeExpr = (SQLMethodInvokeExpr) orderByItem.getExpr();
            checkNvlMethod(methodInvokeExpr);

            Object valueArg = ElasticSqlParseUtil.transferSqlArg(methodInvokeExpr.getParameters().get(1));
            return parseCondition(methodInvokeExpr.getParameters().get(0), queryAs, idfName -> {
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
            });
        }
        throw new ElasticSql2DslException("[syntax error] ElasticSql cannot support sort type: " + orderByItem.getExpr().getClass());
    }

    private void checkNvlMethod(SQLMethodInvokeExpr nvlInvokeExpr) {
        if (!"nvl".equalsIgnoreCase(nvlInvokeExpr.getMethodName())) {
            throw new ElasticSql2DslException("[syntax error] ElasticSql sort condition only support nvl method invoke");
        }

        if (CollectionUtils.isEmpty(nvlInvokeExpr.getParameters()) || nvlInvokeExpr.getParameters().size() > 3) {
            throw new ElasticSql2DslException(String.format("[syntax error] There is no %s args method named nvl",
                    nvlInvokeExpr.getParameters() != null ? nvlInvokeExpr.getParameters().size() : 0));
        }

        SQLExpr fieldArg = nvlInvokeExpr.getParameters().get(0);
        SQLExpr valueArg = nvlInvokeExpr.getParameters().get(1);

        if (!(fieldArg instanceof SQLPropertyExpr) && !(fieldArg instanceof SQLIdentifierExpr)) {
            throw new ElasticSql2DslException("[syntax error] The first arg of nvl method should be field param name");
        }

        if (!(valueArg instanceof SQLCharExpr) && !(valueArg instanceof SQLIntegerExpr) && !(valueArg instanceof SQLNumberExpr)) {
            throw new ElasticSql2DslException("[syntax error] The second arg of nvl method should be number or string");
        }

        if (nvlInvokeExpr.getParameters().size() == 3) {
            SQLExpr sortModArg = nvlInvokeExpr.getParameters().get(2);
            if (!(sortModArg instanceof SQLCharExpr)) {
                throw new ElasticSql2DslException("[syntax error] The third arg of nvl method should be string");
            }
            String sortModeText = ((SQLCharExpr) sortModArg).getText();
            if (!SortOption.AVG.mode().equalsIgnoreCase(sortModeText) && !SortOption.MIN.mode().equalsIgnoreCase(sortModeText)
                    && !SortOption.MAX.mode().equalsIgnoreCase(sortModeText) && !SortOption.SUM.mode().equalsIgnoreCase(sortModeText)) {
                throw new ElasticSql2DslException("[syntax error] The third arg of nvl method should be one of the string[min,max,avg,sum]");
            }
        }
    }

    private SortBuilder parseCondition(SQLExpr sqlExpr, String queryAs, ConditionSortBuilder sortBuilder) {
        List<SortBuilder> tmpSortList = Lists.newLinkedList();
        ElasticSqlIdfParser.parseSqlIdentifier(sqlExpr, queryAs, idfName -> {
            FieldSortBuilder originalSort = sortBuilder.buildSort(idfName);
            tmpSortList.add(originalSort);
        }, (nestPath, idfName) -> {
            FieldSortBuilder originalSort = sortBuilder.buildSort(idfName);
            originalSort.setNestedPath(nestPath);
            tmpSortList.add(originalSort);
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
