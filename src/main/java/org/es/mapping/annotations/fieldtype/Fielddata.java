package org.es.mapping.annotations.fieldtype;

import java.lang.annotation.*;

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD})
@Inherited
public @interface Fielddata {

    /**
     * Can the field use in-memory fielddata for sorting, aggregations, or scripting?
     * Accepts true or false (default).
     */
    boolean enable() default false;

    /**
     * Expert settings which allow to decide which values to load in memory when fielddata is enabled.
     * By default all values are loaded.
     */
    FielddataFrequencyFilter frequency() default @FielddataFrequencyFilter(
            enable = false,
            min = 0d,
            max = 0d,
            min_segment_size = 0
    );


}
