package org.es.mapping.annotations.fieldtype;

import java.lang.annotation.*;

@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD})
public @interface BooleanField {
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
     * Should the field be searchable? Accepts true (default) or false.
     */
    boolean index() default true;

    /**
     * Accepts any of the true or false values listed above. The value is substituted for any explicit null values.
     * Defaults to null, which means the field is treated as missing.
     */
    String null_value() default "";

    /**
     * Whether the field value should be stored and retrievable separately from the _source field.
     * Accepts true or false (default).
     */
    boolean store() default false;
}