package org.es.sql.bean;

import org.elasticsearch.index.query.QueryBuilder;

public class AtomQuery {
    private QueryBuilder query;
    private boolean isNestedQuery;
    private String nestedQueryPath;

    public AtomQuery(QueryBuilder query) {
        this.query = query;
        this.isNestedQuery = false;
    }

    public AtomQuery(QueryBuilder query, String nestedQueryPath) {
        this.query = query;
        this.isNestedQuery = true;
        this.nestedQueryPath = nestedQueryPath;
    }

    public QueryBuilder getQuery() {
        return query;
    }

    public boolean getNestedQuery() {
        return isNestedQuery;
    }

    public String getNestedQueryPath() {
        return nestedQueryPath;
    }
}