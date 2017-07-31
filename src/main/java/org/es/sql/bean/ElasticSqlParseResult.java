package org.es.sql.bean;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.es.sql.utils.ElasticMockClient;


import java.util.List;


public class ElasticSqlParseResult {
    /*取数开始位置*/
    private int from = 0;
    /*取数大小*/
    private int size = 15;
    /*查询索引*/
    private List<String> indices;
    /*查询文档类*/
    private String type;
    /*查询索引别名*/
    private String queryAs;
    /*查询路由值*/
    private List<String> routingBy;
    /*查询字段列表*/
    private List<String> queryFieldList;
    /*SQL的where条件*/
    private transient BoolQueryBuilder whereCondition;
    /*SQL的where条件*/
    private transient BoolQueryBuilder matchCondition;
    /*SQL的order by条件*/
    private transient List<SortBuilder> orderBy;

    private transient boolean isTopStatsAgg = false;

    private transient List<AggregationBuilder> groupBy;

    public List<String> getQueryFieldList() {
        return queryFieldList;
    }

    public void setQueryFieldList(List<String> queryFieldList) {
        this.queryFieldList = queryFieldList;
    }

    public List<String> getIndices() {
        return indices;
    }

    public void setIndices(List<String> indices) {
        this.indices = indices;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public int getFrom() {
        return from;
    }

    public void setFrom(int from) {
        this.from = from;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public String getQueryAs() {
        return queryAs;
    }

    public void setQueryAs(String queryAs) {
        this.queryAs = queryAs;
    }

    public BoolQueryBuilder getWhereCondition() {
        return whereCondition;
    }

    public void setWhereCondition(BoolQueryBuilder whereCondition) {
        this.whereCondition = whereCondition;
    }

    public BoolQueryBuilder getMatchCondition() {
        return matchCondition;
    }

    public void setMatchCondition(BoolQueryBuilder matchCondition) {
        this.matchCondition = matchCondition;
    }

    public List<SortBuilder> getOrderBy() {
        return orderBy;
    }

    public void setOrderBy(List<SortBuilder> orderBy) {
        this.orderBy = orderBy;
    }

    public List<String> getRoutingBy() {
        return routingBy;
    }

    public void setRoutingBy(List<String> routingBy) {
        this.routingBy = routingBy;
    }

    public List<AggregationBuilder> getGroupBy() {
        return groupBy;
    }

    public void setGroupBy(List<AggregationBuilder> groupBy) {
        this.groupBy = groupBy;
    }

    public void setTopStatsAgg() {
        this.isTopStatsAgg = true;
    }

    public boolean getIsTopStatsAgg() {
        return isTopStatsAgg;
    }

    public SearchRequestBuilder toRequest(Client client) {
        SearchRequestBuilder requestBuilder = new SearchRequestBuilder(client, SearchAction.INSTANCE);
        requestBuilder.setFrom(from).setSize(size);

        if (CollectionUtils.isNotEmpty(indices)) {
            requestBuilder.setIndices(indices.toArray(new String[indices.size()]));
        }

        if (StringUtils.isNotBlank(type)) {
            requestBuilder.setTypes(type);
        }

        if (whereCondition != null && whereCondition.hasClauses()) {
            if (matchCondition != null && matchCondition.hasClauses()) {
                requestBuilder.setQuery(matchCondition.filter(whereCondition));
            }
            else {
                requestBuilder.setQuery(QueryBuilders.boolQuery().filter(whereCondition));
            }
        }
        else {
            if (matchCondition != null && matchCondition.hasClauses()) {
                requestBuilder.setQuery(matchCondition);
            }
            else {
                requestBuilder.setQuery(QueryBuilders.matchAllQuery());
            }
        }

        if (CollectionUtils.isNotEmpty(orderBy)) {
            for (SortBuilder sortBuilder : orderBy) {
                requestBuilder.addSort(sortBuilder);
            }
        }

        if (CollectionUtils.isNotEmpty(queryFieldList)) {
            requestBuilder.setFetchSource(queryFieldList.toArray(new String[queryFieldList.size()]), null);
        }

        if (CollectionUtils.isNotEmpty(routingBy)) {
            requestBuilder.setRouting(routingBy.toArray(new String[routingBy.size()]));
        }

        if (CollectionUtils.isNotEmpty(groupBy)) {
            if (!getIsTopStatsAgg()) {
                AggregationBuilder preAgg = null;
                for (AggregationBuilder aggItem : groupBy) {
                    if (preAgg == null) {
                        preAgg = aggItem;
                        continue;
                    }
                    preAgg.subAggregation(aggItem);
                    preAgg = aggItem;
                }
                requestBuilder.addAggregation(groupBy.get(0));
            }
            else {
                for (AggregationBuilder aggItem : groupBy) {
                    requestBuilder.addAggregation(aggItem);
                }
            }
        }

        return requestBuilder;
    }

    public String toDsl(Client client) {
        return toRequest(client).toString();
    }

    public String toDsl() {
        return toDsl(ElasticMockClient.get());
    }

    @Override
    public String toString() {
        String ptn = "index:%s,type:%s,query_as:%s,from:%s,size:%s,routing:%s,dsl:%s";

        return String.format(
                ptn, indices, type, queryAs, from, size,
                (routingBy != null ? routingBy.toString() : "[]"), toDsl()
        );
    }
}
