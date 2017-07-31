package org.es.sql.parser.sql;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLAggregateExpr;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.es.sql.bean.ElasticDslContext;
import org.es.sql.bean.ElasticSqlQueryField;
import org.es.sql.druid.ElasticSqlSelectQueryBlock;
import org.es.sql.enums.QueryFieldType;
import org.es.sql.exception.ElasticSql2DslException;
import org.es.sql.helper.ElasticSqlMethodInvokeHelper;
import org.es.sql.listener.ParseActionListener;


import java.util.List;

public class QuerySelectFieldListParser implements QueryParser {

    private ParseActionListener parseActionListener;

    public QuerySelectFieldListParser(ParseActionListener parseActionListener) {
        this.parseActionListener = parseActionListener;
    }

    @Override
    public void parse(ElasticDslContext dslContext) {
        ElasticSqlSelectQueryBlock queryBlock = (ElasticSqlSelectQueryBlock) dslContext.getQueryExpr().getSubQuery().getQuery();

        List<String> selectFields = Lists.newLinkedList();
        QueryFieldParser queryFieldParser = new QueryFieldParser();
        String queryAs = dslContext.getParseResult().getQueryAs();

        List<AggregationBuilder> aggregations = Lists.newLinkedList();
        for (SQLSelectItem selectField : queryBlock.getSelectList()) {

            // agg method
            if (selectField.getExpr() instanceof SQLAggregateExpr) {

                SQLAggregateExpr aggExpr = (SQLAggregateExpr) selectField.getExpr();
                SQLExpr aggFieldExpr = aggExpr.getArguments().get(0);

                ElasticSqlQueryField aggField = queryFieldParser.parseConditionQueryField(aggFieldExpr, queryAs);
                AbstractAggregationBuilder statsAgg = parseStatsAggregation(aggExpr, aggField.getQueryFieldFullName());

                aggregations.add(statsAgg);
                continue;
            }

            // select field
            ElasticSqlQueryField sqlSelectField = queryFieldParser.parseSelectQueryField(selectField.getExpr(), queryAs);

            if (sqlSelectField.getQueryFieldType() == QueryFieldType.SqlSelectField) {
                selectFields.add(sqlSelectField.getQueryFieldFullName());

                parseActionListener.onSelectFieldParse(sqlSelectField);
            }
        }

        if (CollectionUtils.isNotEmpty(aggregations)) {
            List<AggregationBuilder> groupByList = dslContext.getParseResult().getGroupBy();

            if (CollectionUtils.isNotEmpty(groupByList)) {
                AggregationBuilder lastLevelAggItem = groupByList.get(groupByList.size() - 1);
                for (AggregationBuilder aggItem : aggregations) {
                    lastLevelAggItem.subAggregation(aggItem);
                }
            }
            else {
                dslContext.getParseResult().setTopStatsAgg();
                dslContext.getParseResult().setGroupBy(aggregations);
            }
        }

        dslContext.getParseResult().setQueryFieldList(selectFields);
    }

    private AbstractAggregationBuilder parseStatsAggregation(SQLAggregateExpr aggExpr, String fieldName) {
        ElasticSqlMethodInvokeHelper.checkStatAggMethod(aggExpr);

        String methodName = aggExpr.getMethodName();
        if (ElasticSqlMethodInvokeHelper.AGG_MIN_METHOD.equalsIgnoreCase(methodName)) {
            return AggregationBuilders.min(String.format("%s_%s", ElasticSqlMethodInvokeHelper.AGG_MIN_METHOD, fieldName)).field(fieldName);
        }

        if (ElasticSqlMethodInvokeHelper.AGG_MAX_METHOD.equalsIgnoreCase(methodName)) {
            return AggregationBuilders.max(String.format("%s_%s", ElasticSqlMethodInvokeHelper.AGG_MAX_METHOD, fieldName)).field(fieldName);
        }

        if (ElasticSqlMethodInvokeHelper.AGG_AVG_METHOD.equalsIgnoreCase(methodName)) {
            return AggregationBuilders.avg(String.format("%s_%s", ElasticSqlMethodInvokeHelper.AGG_AVG_METHOD, fieldName)).field(fieldName);
        }

        if (ElasticSqlMethodInvokeHelper.AGG_SUM_METHOD.equalsIgnoreCase(methodName)) {
            return AggregationBuilders.sum(String.format("%s_%s", ElasticSqlMethodInvokeHelper.AGG_SUM_METHOD, fieldName)).field(fieldName);
        }
        throw new ElasticSql2DslException(String.format("[syntax error] UnSupport agg method call[%s]", methodName));
    }
}
