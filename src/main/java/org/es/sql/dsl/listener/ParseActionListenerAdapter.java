package org.es.sql.dsl.listener;

import org.es.sql.dsl.bean.ElasticSqlQueryField;
import org.es.sql.dsl.enums.SQLConditionOperator;

public class ParseActionListenerAdapter implements ParseActionListener {

    @Override
    public void onSelectFieldParse(ElasticSqlQueryField field) {

    }

    @Override
    public void onAtomFilterConditionParse(ElasticSqlQueryField paramName, Object[] paramValues, SQLConditionOperator operator) {

    }

    @Override
    public void onFailure(Throwable t) {

    }
}
