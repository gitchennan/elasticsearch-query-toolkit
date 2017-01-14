package org.elasticsearch.dsl;

public interface ParseActionListener {

    void onSqlSelectFieldParse(ElasticSqlIdentifier sqlIdentifier);

    void onAtomConditionParse(ElasticSqlIdentifier sqlIdentifier, Object[] paramValues, SQLConditionOperator operator);

    void onFailure(Throwable t);
}
