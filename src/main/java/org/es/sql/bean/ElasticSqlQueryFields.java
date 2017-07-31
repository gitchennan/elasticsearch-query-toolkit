package org.es.sql.bean;


import org.es.sql.enums.QueryFieldType;

public class ElasticSqlQueryFields {
    private ElasticSqlQueryFields() {
        // private constructor
    }

    public static ElasticSqlQueryField newSqlSelectField(String fullPathFieldName) {
        return new ElasticSqlQueryField(null, fullPathFieldName, fullPathFieldName, QueryFieldType.SqlSelectField);
    }

    public static ElasticSqlQueryField newMatchAllRootDocField() {
        return new ElasticSqlQueryField(null, "*", "*", QueryFieldType.MatchAllField);
    }

    public static ElasticSqlQueryField newRootDocQueryField(String rootDocFieldName) {
        return new ElasticSqlQueryField(null, rootDocFieldName, rootDocFieldName, QueryFieldType.RootDocField);
    }

    public static ElasticSqlQueryField newInnerDocQueryField(String innerDocFieldPrefix, String innerDocFieldName) {
        String innerDocQueryFieldFullName = String.format("%s.%s", innerDocFieldPrefix, innerDocFieldName);
        return new ElasticSqlQueryField(null, innerDocFieldName, innerDocQueryFieldFullName, QueryFieldType.InnerDocField);
    }

    public static ElasticSqlQueryField newNestedDocQueryField(String nestedDocContextPath, String simpleQueryFieldName) {
        String nestedDocQueryFieldFullName = String.format("%s.%s", nestedDocContextPath, simpleQueryFieldName);
        return new ElasticSqlQueryField(nestedDocContextPath, simpleQueryFieldName, nestedDocQueryFieldFullName, QueryFieldType.NestedDocField);
    }
}
