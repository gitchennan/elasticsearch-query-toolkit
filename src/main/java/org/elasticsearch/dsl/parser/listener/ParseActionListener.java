package org.elasticsearch.dsl.parser.listener;

import org.elasticsearch.dsl.bean.ElasticSqlQueryField;
import org.elasticsearch.dsl.enums.SQLConditionOperator;

public interface ParseActionListener {

    void onSelectFieldParse(ElasticSqlQueryField field);

    void onExactAtomQueryConditionParse(ElasticSqlQueryField paramName, Object[] paramValues, SQLConditionOperator operator);

    void onMatchAtomQueryConditionParse(ElasticSqlQueryField paramName, Object[] parameters);

    void onFailure(Throwable t);
}
