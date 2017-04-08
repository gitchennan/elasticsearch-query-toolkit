package org.es.sql.dsl.bean;

import org.elasticsearch.index.query.FilterBuilder;

public class AtomFilter {
    private FilterBuilder filter;
    private Boolean isNestedFilter;
    private String nestedFilterPathContext;

    public AtomFilter(FilterBuilder filter) {
        this.filter = filter;
        this.isNestedFilter = Boolean.FALSE;
    }

    public AtomFilter(FilterBuilder filter, String nestedFilterPathContext) {
        this.filter = filter;
        this.isNestedFilter = Boolean.TRUE;
        this.nestedFilterPathContext = nestedFilterPathContext;
    }

    public FilterBuilder getFilter() {
        return filter;
    }

    public Boolean getNestedFilter() {
        return isNestedFilter;
    }

    public String getNestedFilterPathContext() {
        return nestedFilterPathContext;
    }
}