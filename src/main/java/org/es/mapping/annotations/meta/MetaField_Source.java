package org.es.mapping.annotations.meta;

import java.lang.annotation.*;

/**
 * The _source field contains the original JSON document body that was passed at index time.
 * The _source field itself is not indexed (and thus is not searchable),
 * but it is stored so that it can be returned when executing fetch requests, like get or search
 * <p>
 * https://www.elastic.co/guide/en/elasticsearch/reference/5.2/mapping-source-field.html
 *
 * @author chennan
 */
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface MetaField_Source {
    /**
     * default set to true
     */
    boolean enabled() default true;


    /**
     * An expert-only feature is the ability to prune the contents of the _source field
     * after the document has been indexed, but before the _source field is stored.
     * <p>
     * Removing fields from the _source has similar downsides to disabling _source,
     * especially the fact that you cannot reindex documents from one Elasticsearch index to another.
     * Consider using source filtering instead.
     * <p>
     * The includes/excludes parameters (which also accept wildcards) can be used as follows:
     * <p>
     * <pre>
     * PUT logs
     * {
     *   "mappings": {
     *     "event": {
     *       "_source": {
     *         "includes": [
     *           "*.count",
     *           "meta.*"
     *         ],
     *         "excludes": [
     *           "meta.description",
     *           "meta.other.*"
     *         ]
     *        }
     *      }
     *    }
     * }
     * </pre>
     */
    String[] includes() default {};

    String[] excludes() default {};
}
