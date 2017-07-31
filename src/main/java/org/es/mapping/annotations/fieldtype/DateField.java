package org.es.mapping.annotations.fieldtype;

import java.lang.annotation.*;

@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD})
public @interface DateField {

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
     * The date format(s) that can be parsed. Defaults to strict_date_optional_time||epoch_millis.
     * <p/>
     * https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-date-format.html
     */
    String format() default "strict_date_optional_time||epoch_millis";

    /**
     * The locale to use when parsing dates since months do not have the same names and/or abbreviations in all languages.
     * The default is the ROOT locale,
     * <p/>
     * https://docs.oracle.com/javase/8/docs/api/java/util/Locale.html#ROOT
     */
    String locale() default "ROOT";

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
}
