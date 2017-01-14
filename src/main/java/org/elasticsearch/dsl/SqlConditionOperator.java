package org.elasticsearch.dsl;

public enum SQLConditionOperator {
    Equality,
    NotEqual,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
    IsNull,
    IsNotNull,
    In,
    NotIn,
    BetweenAnd
}
