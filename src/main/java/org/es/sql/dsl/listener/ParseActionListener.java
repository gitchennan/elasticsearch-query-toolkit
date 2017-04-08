package org.es.sql.dsl.listener;

import org.es.sql.dsl.bean.ElasticSqlQueryField;
import org.es.sql.dsl.enums.SQLConditionOperator;

public interface ParseActionListener {

    void onSelectFieldParse(ElasticSqlQueryField field);

    void onAtomFilterConditionParse(ElasticSqlQueryField paramName, Object[] paramValues, SQLConditionOperator operator);

    void onFailure(Throwable t);
}
