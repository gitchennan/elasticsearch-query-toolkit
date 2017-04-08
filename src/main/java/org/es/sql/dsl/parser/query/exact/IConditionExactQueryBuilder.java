package org.es.sql.dsl.parser.query.exact;

import org.elasticsearch.index.query.FilterBuilder;
import org.es.sql.dsl.enums.SQLConditionOperator;

public interface IConditionExactQueryBuilder {
    FilterBuilder buildQuery(String queryFieldName, SQLConditionOperator operator, Object[] rightParamValues);
}
