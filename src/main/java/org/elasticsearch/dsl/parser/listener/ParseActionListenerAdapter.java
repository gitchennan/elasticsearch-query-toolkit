package org.elasticsearch.dsl.parser.listener;

import org.elasticsearch.dsl.bean.ElasticSqlQueryField;
import org.elasticsearch.dsl.enums.SQLConditionOperator;

public class ParseActionListenerAdapter implements ParseActionListener {

    @Override
    public void onSelectFieldParse(ElasticSqlQueryField field) {

    }

    @Override
    public void onExactAtomQueryConditionParse(ElasticSqlQueryField paramName, Object[] paramValues, SQLConditionOperator operator) {

    }

    @Override
    public void onMatchAtomQueryConditionParse(ElasticSqlQueryField paramName, Object[] parameters) {

    }

    @Override
    public void onFailure(Throwable t) {

    }
}
