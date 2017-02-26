package org.elasticsearch.dsl.parser.listener;

import org.elasticsearch.dsl.enums.SQLConditionOperator;
import org.elasticsearch.dsl.bean.ElasticSqlQueryField;

public class ParseActionListenerAdapter implements ParseActionListener {

    @Override
    public void onSelectFieldParse(ElasticSqlQueryField field) {

    }

    @Override
    public void onAtomConditionParse(ElasticSqlQueryField paramName, Object[] paramValues, SQLConditionOperator operator) {

    }

    @Override
    public void onFailure(Throwable t) {

    }
}
