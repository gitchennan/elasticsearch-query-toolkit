package org.elasticsearch.dsl;

import org.apache.commons.collections.CollectionUtils;
import org.elasticsearch.ElasticMockClient;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.index.query.BoolFilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.utils.GsonHelper;

import java.util.LinkedList;
import java.util.List;

public class ElasticDslContext {
    /*取数开始位置*/
    private int from = 0;
    /*取数大小*/
    private int size = 100;
    /*查询索引*/
    private List<String> indices;
    /*查询文档类*/
    private List<String> types;
    /*查询索引别名*/
    private String queryAs;
    /*SQL的where条件*/
    private BoolFilterBuilder filterBuilder;
    /*SQL的order by条件*/
    private List<SortBuilder> sortBuilderList;
    /*查询字段列表*/
    private List<String> queryFieldList;

    public void addSort(SortBuilder sortBuilder) {
        if (sortBuilder != null) {
            if (sortBuilderList == null) {
                sortBuilderList = new LinkedList<SortBuilder>();
            }
            sortBuilderList.add(sortBuilder);
        }
    }

    public BoolFilterBuilder boolFilter() {
        if (filterBuilder == null) {
            filterBuilder = FilterBuilders.boolFilter();
        }
        return filterBuilder;
    }

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

    public List<String> getTypes() {
        return types;
    }

    public void setTypes(List<String> types) {
        this.types = types;
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

    public String toDsl() {
        SearchRequestBuilder requestBuilder = new SearchRequestBuilder(ElasticMockClient.get());

        if (size > 100) {
            requestBuilder.setFrom(from).setSize(100);
        } else {
            requestBuilder.setFrom(from).setSize(size);
        }

        if (CollectionUtils.isNotEmpty(indices)) {
            requestBuilder.setIndices(indices.toArray(new String[indices.size()]));
        }

        if (CollectionUtils.isNotEmpty(types)) {
            requestBuilder.setTypes(types.toArray(new String[types.size()]));
        }

        if (filterBuilder != null && filterBuilder.hasClauses()) {
            requestBuilder.setQuery(QueryBuilders.filteredQuery(null, filterBuilder));
        } else {
            requestBuilder.setQuery(QueryBuilders.matchAllQuery());
        }

        if (CollectionUtils.isNotEmpty(sortBuilderList)) {
            sortBuilderList.stream().forEach(requestBuilder::addSort);
        }

        if (CollectionUtils.isNotEmpty(queryFieldList)) {
            requestBuilder.setFetchSource(queryFieldList.toArray(new String[queryFieldList.size()]), null);
        }

        return requestBuilder.toString();
    }

    @Override
    public String toString() {
        return GsonHelper.gson.toJson(this) + "\n" + "[DSL]: \n" + toDsl();
    }
}
