package org.elasticsearch.dsl.bean;

import org.elasticsearch.index.query.QueryBuilder;

public class AtomFilter {
    private QueryBuilder filter;
    private Boolean isNestedFilter;
    private String nestedQueryPathContext;

    public AtomFilter(QueryBuilder filter) {
        this.filter = filter;
        this.isNestedFilter = Boolean.FALSE;
    }

    public AtomFilter(QueryBuilder filter, String nestedQueryPathContext) {
        this.filter = filter;
        this.isNestedFilter = Boolean.TRUE;
        this.nestedQueryPathContext = nestedQueryPathContext;
    }

    public QueryBuilder getFilter() {
        return filter;
    }

    public Boolean getNestedFilter() {
        return isNestedFilter;
    }

    public String getNestedFilterPathContext() {
        return nestedQueryPathContext;
    }
}