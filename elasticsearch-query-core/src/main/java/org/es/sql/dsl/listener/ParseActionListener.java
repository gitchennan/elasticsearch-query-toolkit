package org.es.sql.dsl.listener;

import org.es.sql.dsl.bean.ElasticSqlQueryField;
import org.es.sql.dsl.enums.SQLConditionOperator;

public interface ParseActionListener {

    void onSelectFieldParse(ElasticSqlQueryField field);

    void onAtomExactQueryConditionParse(ElasticSqlQueryField paramName, Object[] paramValues, SQLConditionOperator operator);

    void onAtomMethodQueryConditionParse(ElasticSqlQueryField paramName, Object[] parameters);

    void onFailure(Throwable t);
}
