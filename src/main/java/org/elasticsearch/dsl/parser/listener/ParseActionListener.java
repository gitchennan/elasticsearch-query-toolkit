package org.elasticsearch.dsl.parser.listener;

import org.elasticsearch.dsl.enums.SQLConditionOperator;
import org.elasticsearch.dsl.bean.ElasticSqlQueryField;

public interface ParseActionListener {

    void onSelectFieldParse(ElasticSqlQueryField field);

    void onAtomConditionParse(ElasticSqlQueryField paramName, Object[] paramValues, SQLConditionOperator operator);

    void onFailure(Throwable t);
}
