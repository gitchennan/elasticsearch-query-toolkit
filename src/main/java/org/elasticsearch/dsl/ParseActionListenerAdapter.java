package org.elasticsearch.dsl;

import org.elasticsearch.dsl.enums.SQLConditionOperator;

public class ParseActionListenerAdapter implements ParseActionListener {

    @Override
    public void onSelectFieldParse(ElasticSqlIdentifier field) {

    }

    @Override
    public void onAtomConditionParse(ElasticSqlIdentifier paramName, Object[] paramValues, SQLConditionOperator operator) {

    }

    @Override
    public void onFailure(Throwable t) {

    }
}
