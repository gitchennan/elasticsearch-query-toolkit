package org.es.sql.parser.query.exact;

import org.elasticsearch.index.query.QueryBuilder;
import org.es.sql.enums.SQLConditionOperator;

@FunctionalInterface
public interface IConditionExactQueryBuilder {
    QueryBuilder buildQuery(String queryFieldName, SQLConditionOperator operator, Object[] rightParamValues);
}
