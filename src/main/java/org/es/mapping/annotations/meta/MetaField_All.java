package org.es.mapping.annotations.meta;

import java.lang.annotation.*;

/**
 * The _all field is a special catch-all field which concatenates
 * the values of all of the other fields into one big string,
 * using space as a delimiter, which is then analyzed and indexed,
 * but not stored. This means that it can be searched, but not retrieved.
 * <p>
 * The _all field is just a text field, and accepts the same parameters that other string fields accept,
 * including analyzer, term_vectors, index_options, and store.
 * <p>
 * https://www.elastic.co/guide/en/elasticsearch/reference/5.2/mapping-all-field.html
 *
 * @author chennan
 */
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface MetaField_All {
    /**
     * The _all field can be completely disabled per-type by setting enabled to false
     * default set to true
     */
    boolean enabled() default true;

    /**
     * If store is set to true, then the original field value is retrievable and can be highlighted
     * default set to false
     */
    boolean store() default false;
}
