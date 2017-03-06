package org.elasticsearch.dsl.parser.query.method;

import org.elasticsearch.index.query.QueryBuilder;

@FunctionalInterface
public interface IConditionMethodQueryBuilder {
    QueryBuilder buildQuery(String queryFieldName, Object[] parameters);
}
