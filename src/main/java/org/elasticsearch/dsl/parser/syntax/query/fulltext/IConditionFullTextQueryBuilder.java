package org.elasticsearch.dsl.parser.syntax.query.fulltext;

import org.elasticsearch.index.query.QueryBuilder;

@FunctionalInterface
public interface IConditionFullTextQueryBuilder {
    QueryBuilder buildQuery(String queryFieldName, Object[] parameters);
}
