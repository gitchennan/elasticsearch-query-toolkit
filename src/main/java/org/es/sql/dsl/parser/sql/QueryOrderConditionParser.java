package org.es.sql.dsl.parser.sql;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLOrderBy;
import com.alibaba.druid.sql.ast.SQLOrderingSpecification;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.statement.SQLSelectOrderByItem;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.es.sql.druid.ElasticSqlSelectQueryBlock;
import org.es.sql.dsl.bean.ElasticDslContext;
import org.es.sql.dsl.bean.ElasticSqlQueryField;
import org.es.sql.dsl.enums.QueryFieldType;
import org.es.sql.dsl.enums.SortOption;
import org.es.sql.dsl.exception.ElasticSql2DslException;
import org.es.sql.dsl.helper.ElasticSqlMethodInvokeHelper;
import org.es.sql.dsl.listener.ParseActionListener;
import org.es.sql.dsl.parser.query.method.MethodInvocation;

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
            MethodInvocation sortMethodInvocation = new MethodInvocation((SQLMethodInvokeExpr) orderByItem.getExpr(), queryAs, sqlArgs);

            //nvl method
            if (ElasticSqlMethodInvokeHelper.isMethodOf(ElasticSqlMethodInvokeHelper.NVL_METHOD, sortMethodInvocation.getMethodName())) {
                return buildNvlSortBuilder(sortMethodInvocation, orderByItem);
            }

            //script sort
            if (ElasticSqlMethodInvokeHelper.isMethodOf(ElasticSqlMethodInvokeHelper.SCRIPT_SORT_METHOD, sortMethodInvocation.getMethodName())) {
                return buildScriptSortBuilder(sortMethodInvocation, orderByItem);
            }
        }
        throw new ElasticSql2DslException("[syntax error] can not support sort type: " + orderByItem.getExpr().getClass());
    }

    private SortBuilder buildScriptSortBuilder(MethodInvocation scriptSortMethodInvocation, SQLSelectOrderByItem orderByItem) {
        ElasticSqlMethodInvokeHelper.checkScriptSortMethod(scriptSortMethodInvocation);

        String script = scriptSortMethodInvocation.getParameterAsString(0);
        String type = scriptSortMethodInvocation.getParameterAsString(1);

        if (SQLOrderingSpecification.ASC == orderByItem.getType()) {
            return SortBuilders.scriptSort(script, type).order(SortOrder.ASC);
        }
        return SortBuilders.scriptSort(script, type).order(SortOrder.DESC);
    }

    private SortBuilder buildNvlSortBuilder(MethodInvocation sortMethodInvocation, SQLSelectOrderByItem orderByItem) {
        ElasticSqlMethodInvokeHelper.checkNvlMethod(sortMethodInvocation);

        Object valueArg = sortMethodInvocation.getParameterAsObject(1);
        return parseCondition(sortMethodInvocation.getParameter(0), sortMethodInvocation.getQueryAs(), new ConditionSortBuilder() {
            @Override
            public FieldSortBuilder buildSort(String idfName) {
                FieldSortBuilder fieldSortBuilder = null;
                if (SQLOrderingSpecification.ASC == orderByItem.getType()) {
                    fieldSortBuilder = SortBuilders.fieldSort(idfName).order(SortOrder.ASC).missing(valueArg);
                }
                else {
                    fieldSortBuilder = SortBuilders.fieldSort(idfName).order(SortOrder.DESC).missing(valueArg);
                }

                if (sortMethodInvocation.getParameterCount() == 3) {
                    String sortModeText = sortMethodInvocation.getParameterAsString(2);
                    fieldSortBuilder.sortMode(SortOption.get(sortModeText).mode());
                }
                return fieldSortBuilder;
            }
        });
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

    private interface ConditionSortBuilder {
        FieldSortBuilder buildSort(String idfName);
    }
}
