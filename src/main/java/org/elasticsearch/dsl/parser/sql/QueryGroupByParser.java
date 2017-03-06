package org.elasticsearch.dsl.parser.sql;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.druid.sql.ast.statement.SQLSelectGroupByClause;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.elasticsearch.dsl.bean.ElasticDslContext;
import org.elasticsearch.dsl.bean.ElasticSqlQueryField;
import org.elasticsearch.dsl.bean.RangeSegment;
import org.elasticsearch.dsl.enums.QueryFieldType;
import org.elasticsearch.dsl.exception.ElasticSql2DslException;
import org.elasticsearch.dsl.helper.ElasticSqlArgTransferHelper;
import org.elasticsearch.dsl.helper.ElasticSqlMethodInvokeHelper;
import org.elasticsearch.dsl.listener.ParseActionListener;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.range.AbstractRangeBuilder;
import org.elasticsearch.search.aggregations.bucket.range.RangeBuilder;
import org.elasticsearch.search.aggregations.bucket.range.date.DateRangeBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.druid.ElasticSqlSelectQueryBlock;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.Date;
import java.util.List;

public class QueryGroupByParser implements QueryParser {

    private static final Integer MAX_GROUP_BY_SIZE = 500;

    private static final String AGG_BUCKET_KEY_PREFIX = "agg_";

    private ParseActionListener parseActionListener;

    public QueryGroupByParser(ParseActionListener parseActionListener) {
        this.parseActionListener = parseActionListener;
    }

    @Override
    public void parse(ElasticDslContext dslContext) {

        ElasticSqlSelectQueryBlock queryBlock = (ElasticSqlSelectQueryBlock) dslContext.getQueryExpr().getSubQuery().getQuery();
        SQLSelectGroupByClause sqlGroupBy = queryBlock.getGroupBy();
        if (sqlGroupBy != null && CollectionUtils.isNotEmpty(sqlGroupBy.getItems())) {
            String queryAs = dslContext.getParseResult().getQueryAs();

            List<AbstractAggregationBuilder> aggregationList = Lists.newArrayList();
            for (SQLExpr groupByItem : sqlGroupBy.getItems()) {
                if (!(groupByItem instanceof SQLMethodInvokeExpr)) {
                    throw new ElasticSql2DslException("[syntax error] group by item must be an agg method call");
                }
                SQLMethodInvokeExpr aggMethodExpr = (SQLMethodInvokeExpr) groupByItem;

                //Terms Aggregation
                if (ElasticSqlMethodInvokeHelper.AGG_TERMS_METHOD.equalsIgnoreCase(aggMethodExpr.getMethodName())) {
                    ElasticSqlMethodInvokeHelper.checkTermsAggMethod(aggMethodExpr);

                    SQLExpr termsFieldExpr = aggMethodExpr.getParameters().get(0);

                    AggregationBuilder termsBuilder = parseTermsAggregation(queryAs, termsFieldExpr);
                    aggregationList.add(termsBuilder);
                }


                //Range Aggregation
                if (ElasticSqlMethodInvokeHelper.AGG_RANGE_METHOD.equalsIgnoreCase(aggMethodExpr.getMethodName())) {
                    ElasticSqlMethodInvokeHelper.checkRangeAggMethod(aggMethodExpr);

                    List<RangeSegment> rangeSegments = parseRangeSegments(aggMethodExpr, dslContext.getSqlArgs());
                    SQLExpr rangeFieldExpr = aggMethodExpr.getParameters().get(0);

                    AggregationBuilder rangeBuilder = parseRangeAggregation(queryAs, rangeFieldExpr, rangeSegments);
                    aggregationList.add(rangeBuilder);
                }
            }
            dslContext.getParseResult().setGroupBy(aggregationList);
        }

    }

    private AggregationBuilder parseTermsAggregation(String queryAs, SQLExpr termsFieldExpr) {
        QueryFieldParser queryFieldParser = new QueryFieldParser();

        ElasticSqlQueryField queryField = queryFieldParser.parseConditionQueryField(termsFieldExpr, queryAs);
        if(queryField.getQueryFieldType() != QueryFieldType.RootDocField && queryField.getQueryFieldType() != QueryFieldType.InnerDocField) {
            throw new ElasticSql2DslException(String.format("[syntax error] can not support terms aggregation for field type[%s]", queryField.getQueryFieldType()));
        }

        return createTermsBuilder(queryField.getQueryFieldFullName());
    }

    private AggregationBuilder parseRangeAggregation(String queryAs, SQLExpr rangeFieldExpr, List<RangeSegment> rangeSegments) {

        QueryFieldParser queryFieldParser = new QueryFieldParser();

        ElasticSqlQueryField queryField = queryFieldParser.parseConditionQueryField(rangeFieldExpr, queryAs);
        if(queryField.getQueryFieldType() != QueryFieldType.RootDocField && queryField.getQueryFieldType() != QueryFieldType.InnerDocField) {
            throw new ElasticSql2DslException(String.format("[syntax error] can not support range aggregation for field type[%s]", queryField.getQueryFieldType()));
        }

        return createRangeBuilder(queryField.getQueryFieldFullName(), rangeSegments);
    }

    private List<RangeSegment> parseRangeSegments(SQLMethodInvokeExpr rangeMethodExpr, Object[] args) {
        List<RangeSegment> rangeSegmentList = Lists.newArrayList();
        for (int pIdx = 1; pIdx < rangeMethodExpr.getParameters().size(); pIdx++) {
            SQLMethodInvokeExpr segMethodExpr = (SQLMethodInvokeExpr) rangeMethodExpr.getParameters().get(pIdx);

            ElasticSqlMethodInvokeHelper.checkRangeItemAggMethod(segMethodExpr);

            Object from = ElasticSqlArgTransferHelper.transferSqlArg(segMethodExpr.getParameters().get(0), args, true);
            Object to = ElasticSqlArgTransferHelper.transferSqlArg(segMethodExpr.getParameters().get(1), args, true);

            rangeSegmentList.add(new RangeSegment(from, to,
                    from instanceof Number ? RangeSegment.SegmentType.Numeric : RangeSegment.SegmentType.Date));
        }
        return rangeSegmentList;
    }

    private TermsBuilder createTermsBuilder(String termsFieldName) {
        return AggregationBuilders.terms(AGG_BUCKET_KEY_PREFIX + termsFieldName)
                .field(termsFieldName)
                .minDocCount(1).shardMinDocCount(1)
                .shardSize(MAX_GROUP_BY_SIZE << 1).size(MAX_GROUP_BY_SIZE).order(Terms.Order.count(false));
    }

    private AbstractRangeBuilder createRangeBuilder(String rangeFieldName, List<RangeSegment> rangeSegments) {
        AbstractRangeBuilder rangeBuilder = null;
        RangeSegment.SegmentType segType = rangeSegments.get(0).getSegmentType();

        if (segType == RangeSegment.SegmentType.Numeric) {
            RangeBuilder numericRangeBuilder = AggregationBuilders.range(AGG_BUCKET_KEY_PREFIX + rangeFieldName).field(rangeFieldName);
            for (RangeSegment segment : rangeSegments) {
                String key = String.format("%s-%s", segment.getFrom().toString(), segment.getTo().toString());
                numericRangeBuilder.addRange(key, Double.valueOf(segment.getFrom().toString()), Double.valueOf(segment.getTo().toString()));
            }
            rangeBuilder = numericRangeBuilder;
        }

        if (segType == RangeSegment.SegmentType.Date) {
            DateRangeBuilder dateRangeBuilder = AggregationBuilders.dateRange(AGG_BUCKET_KEY_PREFIX + rangeFieldName).field(rangeFieldName);
            for (RangeSegment segment : rangeSegments) {

                Date fromDate = getDateRangeVal(segment.getFrom().toString());
                Date toDate = getDateRangeVal(segment.getTo().toString());

                String key = String.format("[%s]-[%s]", formatDateRangeAggKey(fromDate), formatDateRangeAggKey(toDate));
                dateRangeBuilder.addRange(key, segment.getFrom(), segment.getTo());
            }
            rangeBuilder = dateRangeBuilder;
        }
        return rangeBuilder;
    }

    private String formatDateRangeAggKey(Date date) {
        final String dateRangeKeyPattern = "yyyy-MM-dd HH:mm:ss";
        return new DateTime(date).toString(dateRangeKeyPattern);
    }

    public static Date getDateRangeVal(String date) {
        final String dateRangeValPattern = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";
        DateTimeFormatter formatter = DateTimeFormat.forPattern(dateRangeValPattern);
        return formatter.parseDateTime(date).toDate();
    }
}
