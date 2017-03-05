package org.elasticsearch.dsl.bean;

import org.elasticsearch.index.query.QueryBuilder;

public class AtomQuery {
    private QueryBuilder query;
    private Boolean isNestedQuery;
    private String nestedQueryPathContext;

    public AtomQuery(QueryBuilder query) {
        this.query = query;
        this.isNestedQuery = Boolean.FALSE;
    }

    public AtomQuery(QueryBuilder query, String nestedQueryPathContext) {
        this.query = query;
        this.isNestedQuery = Boolean.TRUE;
        this.nestedQueryPathContext = nestedQueryPathContext;
    }

    public QueryBuilder getQuery() {
        return query;
    }

    public Boolean getNestedQuery() {
        return isNestedQuery;
    }

    public String getNestedQueryPathContext() {
        return nestedQueryPathContext;
    }
}