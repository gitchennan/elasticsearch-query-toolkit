package org.elasticsearch.dsl.parser.listener;

import org.elasticsearch.dsl.bean.ElasticSqlQueryField;
import org.elasticsearch.dsl.enums.SQLConditionOperator;

public class ParseActionListenerAdapter implements ParseActionListener {

    @Override
    public void onSelectFieldParse(ElasticSqlQueryField field) {

    }

    @Override
    public void onAtomExactQueryConditionParse(ElasticSqlQueryField paramName, Object[] paramValues, SQLConditionOperator operator) {

    }

    @Override
    public void onAtomMethodQueryConditionParse(ElasticSqlQueryField paramName, Object[] parameters) {

    }

    @Override
    public void onFailure(Throwable t) {

    }
}
