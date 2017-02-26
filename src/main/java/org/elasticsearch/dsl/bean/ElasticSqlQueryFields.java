package org.elasticsearch.dsl.bean;

import org.elasticsearch.dsl.enums.QueryFieldType;

public class ElasticSqlQueryFields {
    private ElasticSqlQueryFields() {
        // private constructor
    }

    public static ElasticSqlQueryField newMatchAllField(String prefixPath) {
        String matchAllField = String.format("%s.%s", prefixPath, "*");
        return new ElasticSqlQueryField(null, matchAllField, matchAllField, QueryFieldType.MatchAllField);
    }

    public static ElasticSqlQueryField newRootDocQueryField(String rootDocFieldName) {
        return new ElasticSqlQueryField(null, rootDocFieldName, rootDocFieldName, QueryFieldType.RootDocField);
    }

    public static ElasticSqlQueryField newInnerDocQueryField(String innerDocQueryFieldFullName) {
        return new ElasticSqlQueryField(null, innerDocQueryFieldFullName, innerDocQueryFieldFullName, QueryFieldType.InnerDocField);
    }

    public static ElasticSqlQueryField newInnerDocQueryField(String innerDocFieldPrefix, String innerDocFieldName) {
        String innerDocQueryFieldFullName = String.format("%s.%s", innerDocFieldPrefix, innerDocFieldName);
        return newInnerDocQueryField(innerDocQueryFieldFullName);
    }

    public static ElasticSqlQueryField newNestedDocQueryField(String nestedDocContextPath, String simpleQueryFieldName) {
        return new ElasticSqlQueryField(nestedDocContextPath, simpleQueryFieldName, String.format("%s.%s", nestedDocContextPath, simpleQueryFieldName), QueryFieldType.NestedDocField);
    }
}
