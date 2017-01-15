package org.elasticsearch.dsl;

public interface ParseActionListener {

    void onSelectFieldParse(ElasticSqlIdentifier field);

    void onAtomConditionParse(ElasticSqlIdentifier paramName, Object[] paramValues, SQLConditionOperator operator);

    void onFailure(Throwable t);
}
