package org.es.sql.bean;

import org.es.sql.enums.QueryFieldType;

public class ElasticSqlQueryField {
    /*[内嵌文档]的上下文路径*/
    private String nestedDocContextPath;
    /*字段名(不包含前缀)*/
    private String simpleQueryFieldName;
    /*字段名(包含全限定名)*/
    private String queryFieldFullName;
    /*字段类型*/
    private QueryFieldType queryFieldType;

    public ElasticSqlQueryField(String nestedDocContextPath, String simpleQueryFieldName, String queryFieldFullName, QueryFieldType queryFieldType) {
        this.nestedDocContextPath = nestedDocContextPath;
        this.simpleQueryFieldName = simpleQueryFieldName;
        this.queryFieldFullName = queryFieldFullName;
        this.queryFieldType = queryFieldType;
    }

    public String getNestedDocContextPath() {
        return nestedDocContextPath;
    }

    public String getSimpleQueryFieldName() {
        return simpleQueryFieldName;
    }

    public String getQueryFieldFullName() {
        return queryFieldFullName;
    }

    public QueryFieldType getQueryFieldType() {
        return queryFieldType;
    }
}
