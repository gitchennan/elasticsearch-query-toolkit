package org.es.mapping.annotations.fieldtype;

import java.lang.annotation.*;

/**
 * Fielddata filtering can be used to reduce the number of terms loaded into memory,
 * and thus reduce memory usage. Terms can be filtered by frequency:
 * <p/>
 * The frequency filter allows you to only load terms whose document frequency falls between a min and max value,
 * which can be expressed an absolute number (when the number is bigger than 1.0) or as a percentage (eg 0.01 is 1% and 1.0 is 100%).
 * Frequency is calculated per segment. Percentages are based on the number of docs which have a value for the field,
 * as opposed to all docs in the segment.
 * <p/>
 * Small segments can be excluded completely by specifying the minimum number of docs that the segment should contain with min_segment_size
 * <p/>
 * https://www.elastic.co/guide/en/elasticsearch/reference/current/fielddata.html#field-data-filtering
 *
 * @author chennan
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD})
@Inherited
public @interface FielddataFrequencyFilter {

    boolean enable() default false;

    double min();

    double max();

    int min_segment_size();
}
