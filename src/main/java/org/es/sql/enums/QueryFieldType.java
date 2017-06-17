package org.es.sql.enums;

public enum QueryFieldType {
    MatchAllField,
    SqlSelectField,

    RootDocField,
    InnerDocField,
    NestedDocField,
}