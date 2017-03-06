package org.elasticsearch.dsl.listener;

import org.elasticsearch.dsl.bean.ElasticSqlQueryField;
import org.elasticsearch.dsl.enums.SQLConditionOperator;

public interface ParseActionListener {

    void onSelectFieldParse(ElasticSqlQueryField field);

    void onAtomExactQueryConditionParse(ElasticSqlQueryField paramName, Object[] paramValues, SQLConditionOperator operator);

    void onAtomMethodQueryConditionParse(ElasticSqlQueryField paramName, Object[] parameters);

    void onFailure(Throwable t);
}
