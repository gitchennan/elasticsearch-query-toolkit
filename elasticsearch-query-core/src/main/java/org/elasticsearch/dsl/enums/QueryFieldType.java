package org.elasticsearch.dsl.enums;

public enum QueryFieldType {
    MatchAllField,
    SqlSelectField,

    RootDocField,
    InnerDocField,
    NestedDocField,
}