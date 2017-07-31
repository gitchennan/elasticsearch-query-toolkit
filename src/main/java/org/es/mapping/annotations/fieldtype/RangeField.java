package org.es.mapping.annotations.fieldtype;


import org.es.mapping.annotations.enums.RangeType;

import java.lang.annotation.*;

@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD})
public @interface RangeField {
    /**
     * type of range
     * <p>
     * {@link RangeType}
     */
    RangeType type();

    /**
     * The date format(s) that can be parsed. Defaults to strict_date_optional_time||epoch_millis.
     * <p>
     * https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-date-format.html
     */
    String format() default "strict_date_optional_time||epoch_millis";

    /**
     * Try to convert strings to numbers and truncate fractions for integers.
     * Accepts true (default) and false.
     */
    boolean coerce() default true;

    /**
     * Mapping field-level query time boosting. Accepts a floating point number, defaults to 1.0.
     */
    float boost() default 1.0f;

    /**
     * Whether or not the field value should be included in the _all field?
     * Accepts true or false. Defaults to false if index is set to no,
     * or if a parent object field sets include_in_all to false. Otherwise defaults to true.
     */
    boolean include_in_all() default true;

    /**
     * Should the field be searchable? Accepts true (default) or false.
     */
    boolean index() default true;


    /**
     * Whether the field value should be stored and retrievable separately from the _source field.
     * Accepts true or false (default).
     */
    boolean store() default false;
}