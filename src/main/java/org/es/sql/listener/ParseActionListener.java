package org.es.sql.listener;

import org.es.sql.bean.ElasticSqlQueryField;
import org.es.sql.enums.SQLConditionOperator;

import java.util.List;

public interface ParseActionListener {

    void onSelectFieldParse(ElasticSqlQueryField field);

    void onAtomExactQueryConditionParse(ElasticSqlQueryField paramName, Object[] params, SQLConditionOperator operator);

    void onAtomMethodQueryConditionParse(ElasticSqlQueryField paramName, Object[] params);

    void onRoutingValuesParse(List<String> routingValues);

    void onLimitSizeParse(int from, int size);
}
