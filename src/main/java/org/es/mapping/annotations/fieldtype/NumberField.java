package org.es.mapping.annotations.fieldtype;


import org.es.mapping.annotations.enums.NumberType;

import java.lang.annotation.*;

@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD})
public @interface NumberField {
    /**
     * type of number.
     * <p/>
     * {@link NumberType}
     */
    NumberType type();

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
     * Should the field be stored on disk in a column-stride fashion,
     * so that it can later be used for sorting, aggregations, or scripting?
     * Accepts true (default) or false.
     */
    boolean doc_values() default true;

    /**
     * If true, malformed numbers are ignored. If false (default),
     * malformed numbers throw an exception and reject the whole document.
     */
    boolean ignore_malformed() default false;

    /**
     * Whether or not the field value should be included in the _all field? Accepts true or false.
     * Defaults to false if index is set to false, or if a parent object field sets include_in_all to false.
     * Otherwise defaults to true.
     */
    boolean include_in_all() default true;

    /**
     * Should the field be searchable? Accepts true (default) and false.
     */
    boolean index() default true;

    /**
     * Accepts a numeric value of the same type as the field which is substituted for any explicit null values.
     * Defaults to null, which means the field is treated as missing.
     */
    String null_value() default "";

    /**
     * Whether the field value should be stored and retrievable separately from
     * the _source field. Accepts true or false (default).
     */
    boolean store() default false;


    /**
     * scaled_float accepts an additional parameter:
     * <p/>
     * The scaling factor to use when encoding values. Values will be multiplied by this factor at index time and rounded to the closest long value.
     * For instance, a scaled_float with a scaling_factor of 10 would internally store 2.34 as 23 and all
     * search-time operations (queries, aggregations, sorting) will behave as if the document had a value of 2.3.
     * High values of scaling_factor improve accuracy but also increase space requirements. This parameter is required.
     */
    int scaling_factor() default 1;
}
