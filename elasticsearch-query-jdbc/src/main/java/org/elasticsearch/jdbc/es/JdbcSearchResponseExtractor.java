package org.elasticsearch.jdbc.es;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import org.apache.commons.collections.CollectionUtils;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.elasticsearch.search.aggregations.bucket.range.date.InternalDateRange;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.search.aggregations.metrics.min.Min;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;

import java.util.List;
import java.util.Map;

public class JdbcSearchResponseExtractor {

    private static final String AGG_RESULT_COUNT_FIELD = "_count";

    public String extractSearchResponse(SearchResponse searchResponse) {
        JdbcSearchResponse<String> jdbcSearchResponse = null;

        if (Boolean.TRUE == isAggregationResponse(searchResponse)) {
            jdbcSearchResponse = extractAggregationResult(searchResponse);
        }
        else {
            jdbcSearchResponse = extractQueryResult(searchResponse);
        }
        setBasicResponseInfo(searchResponse, jdbcSearchResponse);

        return jdbcSearchResponse.toJson();
    }

    private boolean isAggregationResponse(SearchResponse response) {
        if (response.getAggregations() == null) {
            return false;
        }
        List<Aggregation> aggList = response.getAggregations().asList();
        return CollectionUtils.isNotEmpty(aggList);
    }

    private void setBasicResponseInfo(SearchResponse searchResponse, JdbcSearchResponse<String> jdbcResponse) {
        jdbcResponse.setFailedShards(searchResponse.getFailedShards());
        jdbcResponse.setSuccessfulShards(searchResponse.getSuccessfulShards());
        jdbcResponse.setTookInMillis(searchResponse.getTookInMillis());
        jdbcResponse.setTotalShards(searchResponse.getTotalShards());
    }

    private JdbcSearchResponse<String> extractAggregationResult(SearchResponse searchResponse) {
        JdbcSearchResponse<String> jdbcSearchResponse = new JdbcSearchResponse<String>();
        List<Aggregation> aggList = searchResponse.getAggregations().asList();

        Gson defaultGson = new Gson();
        List<String> resultList = Lists.newArrayList();
        dfsParseAggregationResponse(aggList, Maps.newTreeMap(), resultList, searchResponse.getHits().getTotalHits(), defaultGson);

        jdbcSearchResponse.setResultList(resultList);

        long totalAggCount = resultList.size();
        jdbcSearchResponse.setTotalCount(totalAggCount);

        return jdbcSearchResponse;
    }

    private JdbcSearchResponse<String> extractQueryResult(SearchResponse searchResponse) {
        JdbcSearchResponse<String> jdbcSearchResponse = new JdbcSearchResponse<String>();
        jdbcSearchResponse.setTotalCount(searchResponse.getHits().getTotalHits());

        if (searchResponse.getHits() != null && searchResponse.getHits().getHits() != null) {
            List<String> hits = Lists.newLinkedList();
            for (SearchHit searchHit : searchResponse.getHits().getHits()) {
                hits.add(searchHit.getSourceAsString());
            }
            jdbcSearchResponse.setResultList(hits);
        }
        return jdbcSearchResponse;
    }

    private void dfsParseAggregationResponse(List<Aggregation> aggregations, Map<String, Object> resultObject,
                                             List<String> resultList, Long preBucketCount, Gson gson) {
        if (preBucketCount <= 0) {
            return;
        }
        // if no stats agg, return count
        if (CollectionUtils.isEmpty(aggregations)) {
            resultObject.put(AGG_RESULT_COUNT_FIELD, preBucketCount);
            resultList.add(gson.toJson(resultObject));
            resultObject.remove(AGG_RESULT_COUNT_FIELD);
            return;
        }
        // process stats agg
        if (isStatsAgg(aggregations.get(0))) {
            List<String> statsKeys = Lists.newArrayList();
            for (Aggregation aggregation : aggregations) {
                //stats aggregation
                parseStatsAgg(aggregation, resultObject, statsKeys);
            }

            resultObject.put(AGG_RESULT_COUNT_FIELD, preBucketCount);
            statsKeys.add(AGG_RESULT_COUNT_FIELD);

            resultList.add(gson.toJson(resultObject));
            for (String keyName : statsKeys) {
                resultObject.remove(keyName);
            }
            return;
        }
        // process bucket agg
        for (Aggregation aggregation : aggregations) {
            //terms aggregation
            if (aggregation instanceof Terms) {
                Terms rootTerms = (Terms) aggregation;
                for (Terms.Bucket bucket : rootTerms.getBuckets()) {
                    resultObject.put(rootTerms.getName(), bucket.getKey());
                    dfsParseAggregationResponse(bucket.getAggregations().asList(), resultObject, resultList, bucket.getDocCount(), gson);
                    resultObject.remove(rootTerms.getName());
                }
            }
            //range aggregation
            if (aggregation instanceof Range) {
                if (aggregation instanceof InternalDateRange) {
                    InternalDateRange rootRange = (InternalDateRange) aggregation;
                    for (InternalDateRange.Bucket bucket : rootRange.getBuckets()) {
                        resultObject.put(rootRange.getName(), bucket.getKey());
                        dfsParseAggregationResponse(bucket.getAggregations().asList(), resultObject, resultList, bucket.getDocCount(), gson);
                        resultObject.remove(rootRange.getName());
                    }
                }
                else {
                    Range rootRange = (Range) aggregation;
                    for (Range.Bucket bucket : rootRange.getBuckets()) {
                        resultObject.put(rootRange.getName(), bucket.getKey());
                        dfsParseAggregationResponse(bucket.getAggregations().asList(), resultObject, resultList, bucket.getDocCount(), gson);
                        resultObject.remove(rootRange.getName());
                    }
                }
            }
        }
    }

    private void parseStatsAgg(Aggregation aggregation, Map<String, Object> resultObject, List<String> statsKeys) {
        if (aggregation instanceof Min) {
            Min stats = (Min) aggregation;
            resultObject.put(stats.getName(), getDoubleVal(stats.getValue()));
            statsKeys.add(stats.getName());
        }

        if (aggregation instanceof Max) {
            Max stats = (Max) aggregation;
            resultObject.put(stats.getName(), getDoubleVal(stats.getValue()));
            statsKeys.add(stats.getName());
        }

        if (aggregation instanceof Sum) {
            Sum stats = (Sum) aggregation;
            resultObject.put(stats.getName(), getDoubleVal(stats.getValue()));
            statsKeys.add(stats.getName());
        }

        if (aggregation instanceof Avg) {
            Avg stats = (Avg) aggregation;
            resultObject.put(stats.getName(), getDoubleVal(stats.getValue()));
            statsKeys.add(stats.getName());
        }
    }

    private boolean isStatsAgg(Aggregation aggregation) {
        return aggregation instanceof Min
                || aggregation instanceof Max
                || aggregation instanceof Avg
                || aggregation instanceof Sum;
    }

    private double getDoubleVal(Double d) {
        return d.isNaN() || d.isInfinite() ? 0 : d;
    }
}
