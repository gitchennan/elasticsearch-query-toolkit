package org.elasticsearch.dsl.parser.syntax.query.exact;

import org.elasticsearch.dsl.enums.SQLConditionOperator;
import org.elasticsearch.index.query.QueryBuilder;

@FunctionalInterface
public interface IConditionExactQueryBuilder {
    QueryBuilder buildQuery(String queryFieldName, SQLConditionOperator operator, Object[] rightParamValues);
}
