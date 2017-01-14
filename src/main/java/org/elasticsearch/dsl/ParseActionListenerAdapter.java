package org.elasticsearch.dsl;

public class ParseActionListenerAdapter implements ParseActionListener {

    @Override
    public void onSqlSelectFieldParse(ElasticSqlIdentifier sqlIdentifier) {

    }

    @Override
    public void onAtomConditionParse(ElasticSqlIdentifier sqlIdentifier, Object[] paramValues, SQLConditionOperator operator) {

    }

    @Override
    public void onFailure(Throwable t) {

    }
}
