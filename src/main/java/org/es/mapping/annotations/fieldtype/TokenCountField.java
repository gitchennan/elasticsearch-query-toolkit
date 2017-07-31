package org.es.mapping.annotations.fieldtype;

import java.lang.annotation.*;

@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD})
public @interface TokenCountField {

    String name();

    /**
     * The analyzer which should be used to analyze the string value.
     * Required. For best performance, use an analyzer without token filters.
     */
    String analyzer();

    /**
     * Indicates if position increments should be counted.
     * Set to false if you don��t want to count tokens removed by analyzer filters (like stop). Defaults to true.
     */
    boolean enable_position_increments() default true;

    /**
     * Mapping field-level query time boosting. Accepts a floating point number, defaults to 1.0.
     */
    float boost() default 1.0f;

    /**
     * Should the field be stored on disk in a column-stride fashion,
     * so that it can later be used for sorting, aggregations, or scripting?
     * Accepts true (default) or false.
     */
    boolean doc_values() default true;

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
     * Accepts a string value which is substituted for any explicit null values.
     * Defaults to null, which means the field is treated as missing.
     */
    String null_value() default "";

    /**
     * Whether the field value should be stored and retrievable separately from the _source field.
     * Accepts true or false (default).
     */
    boolean store() default false;
}


