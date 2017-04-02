package org.es.sql.dsl.parser.sql;

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
import org.es.sql.druid.ElasticSqlSelectQueryBlock;
import org.es.sql.dsl.bean.ElasticDslContext;
import org.es.sql.dsl.bean.ElasticSqlQueryField;
import org.es.sql.dsl.enums.QueryFieldType;
import org.es.sql.dsl.enums.SortOption;
import org.es.sql.dsl.exception.ElasticSql2DslException;
import org.es.sql.dsl.helper.ElasticSqlArgTransferHelper;
import org.es.sql.dsl.helper.ElasticSqlMethodInvokeHelper;
import org.es.sql.dsl.listener.ParseActionListener;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;

import java.util.List;

public class QueryOrderConditionParser implements QueryParser {

    private ParseActionListener parseActionListener;

    public QueryOrderConditionParser(ParseActionListener parseActionListener) {
        this.parseActionListener = parseActionListener;
    }

    @Override
    public void parse(ElasticDslContext dslContext) {
        ElasticSqlSelectQueryBlock queryBlock = (ElasticSqlSelectQueryBlock) dslContext.getQueryExpr().getSubQuery().getQuery();
        SQLOrderBy sqlOrderBy = queryBlock.getOrderBy();
        if (sqlOrderBy != null && CollectionUtils.isNotEmpty(sqlOrderBy.getItems())) {
            List<SortBuilder> orderByList = Lists.newLinkedList();
            for (SQLSelectOrderByItem orderByItem : sqlOrderBy.getItems()) {
                SortBuilder orderBy = parseOrderCondition(orderByItem, dslContext.getParseResult().getQueryAs(), dslContext.getSqlArgs());
                if (orderBy != null) {
                    orderByList.add(orderBy);
                }
            }
            dslContext.getParseResult().setOrderBy(orderByList);
        }
    }

    private SortBuilder parseOrderCondition(SQLSelectOrderByItem orderByItem, String queryAs, Object[] sqlArgs) {
        if (orderByItem.getExpr() instanceof SQLPropertyExpr || orderByItem.getExpr() instanceof SQLIdentifierExpr) {
            return parseCondition(orderByItem.getExpr(), queryAs, new ConditionSortBuilder() {
                @Override
                public FieldSortBuilder buildSort(String queryFieldName) {
                    if (SQLOrderingSpecification.ASC == orderByItem.getType()) {
                        return SortBuilders.fieldSort(queryFieldName).order(SortOrder.ASC);
                    }
                    else {
                        return SortBuilders.fieldSort(queryFieldName).order(SortOrder.DESC);
                    }
                }
            });
        }
        if (orderByItem.getExpr() instanceof SQLMethodInvokeExpr) {
            SQLMethodInvokeExpr methodInvokeExpr = (SQLMethodInvokeExpr) orderByItem.getExpr();
            //nvl method
            if (ElasticSqlMethodInvokeHelper.isMethodOf(ElasticSqlMethodInvokeHelper.NVL_METHOD, methodInvokeExpr.getMethodName())) {
                ElasticSqlMethodInvokeHelper.checkNvlMethod(methodInvokeExpr);
                Object valueArg = ElasticSqlArgTransferHelper.transferSqlArg(methodInvokeExpr.getParameters().get(1), sqlArgs);
                return parseCondition(methodInvokeExpr.getParameters().get(0), queryAs, new ConditionSortBuilder() {
                    @Override
                    public FieldSortBuilder buildSort(String idfName) {
                        FieldSortBuilder fieldSortBuilder = null;

                        if (SQLOrderingSpecification.ASC == orderByItem.getType()) {
                            fieldSortBuilder = SortBuilders.fieldSort(idfName).order(SortOrder.ASC).missing(valueArg);
                        }
                        else {
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
        }
        throw new ElasticSql2DslException("[syntax error] can not support sort type: " + orderByItem.getExpr().getClass());
    }

    private SortBuilder parseCondition(SQLExpr sqlExpr, String queryAs, ConditionSortBuilder sortBuilder) {
        QueryFieldParser queryFieldParser = new QueryFieldParser();
        ElasticSqlQueryField sortField = queryFieldParser.parseConditionQueryField(sqlExpr, queryAs);

        SortBuilder rtnSortBuilder = null;
        if (sortField.getQueryFieldType() == QueryFieldType.RootDocField || sortField.getQueryFieldType() == QueryFieldType.InnerDocField) {
            rtnSortBuilder = sortBuilder.buildSort(sortField.getQueryFieldFullName());
        }

        if (sortField.getQueryFieldType() == QueryFieldType.NestedDocField) {
            FieldSortBuilder originalSort = sortBuilder.buildSort(sortField.getQueryFieldFullName());
            originalSort.setNestedPath(sortField.getNestedDocContextPath());
            rtnSortBuilder = originalSort;
        }

        if (rtnSortBuilder == null) {
            throw new ElasticSql2DslException(String.format("[syntax error] sort condition field can not support type[%s]", sortField.getQueryFieldType()));
        }

        return rtnSortBuilder;
    }


    @FunctionalInterface
    private interface ConditionSortBuilder {
        FieldSortBuilder buildSort(String idfName);
    }
}
