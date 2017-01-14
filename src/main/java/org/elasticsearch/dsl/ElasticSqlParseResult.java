package org.elasticsearch.dsl;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.BoolFilterBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.util.ElasticMockClient;

import java.util.List;
import java.util.function.Consumer;

public class ElasticSqlParseResult {
    /*取数开始位置*/
    private int from = 0;
    /*取数大小*/
    private int size = 15;
    /*查询索引*/
    private String index;
    /*查询文档类*/
    private String type;
    /*查询索引别名*/
    private String queryAs;
    /*查询路由值*/
    private List<String> routingBy;
    /*查询字段列表*/
    private List<String> queryFieldList;
    /*SQL的where条件*/
    private transient BoolFilterBuilder whereCondition;
    /*SQL的order by条件*/
    private transient List<SortBuilder> orderBy;

    public List<String> getQueryFieldList() {
        return queryFieldList;
    }

    public void setQueryFieldList(List<String> queryFieldList) {
        this.queryFieldList = queryFieldList;
    }

    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
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

    public BoolFilterBuilder getWhereCondition() {
        return whereCondition;
    }

    public void setWhereCondition(BoolFilterBuilder whereCondition) {
        this.whereCondition = whereCondition;
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

    public SearchRequestBuilder toRequest(Client client) {
        final SearchRequestBuilder requestBuilder = new SearchRequestBuilder(client);
        requestBuilder.setFrom(from).setSize(size);

        if (StringUtils.isNotBlank(index)) {
            requestBuilder.setIndices(index);
        }

        if (StringUtils.isNotBlank(type)) {
            requestBuilder.setTypes(type);
        }

        if (whereCondition != null && whereCondition.hasClauses()) {
            requestBuilder.setQuery(QueryBuilders.filteredQuery(null, whereCondition));
        } else {
            requestBuilder.setQuery(QueryBuilders.matchAllQuery());
        }

        if (CollectionUtils.isNotEmpty(orderBy)) {
            orderBy.stream().forEach(new Consumer<SortBuilder>() {
                @Override
                public void accept(SortBuilder sortBuilder) {
                    requestBuilder.addSort(sortBuilder);
                }
            });
        }

        if (CollectionUtils.isNotEmpty(queryFieldList)) {
            requestBuilder.setFetchSource(queryFieldList.toArray(new String[queryFieldList.size()]), null);
        }

        if (CollectionUtils.isNotEmpty(routingBy)) {
            requestBuilder.setRouting(routingBy.toArray(new String[routingBy.size()]));
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
        return String.format(ptn, index, type, queryAs, from, size, routingBy != null ? routingBy.toString() : "[]", toDsl());
    }
}
