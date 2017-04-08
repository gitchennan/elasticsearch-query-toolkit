package org.es.sql.dsl.enums;

public enum QueryFieldType {
    MatchAllField,
    SqlSelectField,

    RootDocField,
    InnerDocField,
    NestedDocField,
}