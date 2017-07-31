package org.es.mapping.annotations;

import org.es.mapping.annotations.meta.MetaField_All;
import org.es.mapping.annotations.meta.MetaField_Parent;
import org.es.mapping.annotations.meta.MetaField_Routing;
import org.es.mapping.annotations.meta.MetaField_Source;

import java.lang.annotation.*;

@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface TypeSetting {

    /**
     * By default, fields can be added dynamically to a document,
     * or to inner objects within a document,
     * just by indexing a document containing the new field.
     */
    boolean dynamic() default true;

    /**
     * the type of document
     */
    String _type();

    /**
     * The _all field is a special catch-all field which concatenates
     * the values of all of the other fields into one big string,
     * using space as a delimiter, which is then analyzed and indexed,
     * but not stored. This means that it can be searched, but not retrieved
     */
    MetaField_All _all() default @MetaField_All(enabled = true, store = false);

    /**
     * parent type
     */
    MetaField_Parent _parent() default @MetaField_Parent(parentClass = {});

    /**
     * A document is routed to a particular shard in an index using the following formula:
     * <pre>shard_num = hash(_routing) % num_primary_shards</pre>
     * <p>
     * Forgetting the routing value can lead to a document being indexed on more than one shard.
     * As a safeguard, the _routing field can be configured to make a custom routing value required for all CRUD operations
     */
    MetaField_Routing _routing() default @MetaField_Routing(required = false);


    /**
     * The _source field contains the original JSON document body that was passed at index time.
     * The _source field itself is not indexed (and thus is not searchable),
     * but it is stored so that it can be returned when executing fetch requests, like get or search
     */
    MetaField_Source _source() default @MetaField_Source(enabled = true);

    /**
     * Each mapping type can have custom meta data associated with it.
     * These are not used at all by Elasticsearch, but can be used to store application-specific metadata,
     * such as the class that a document belongs to:
     * <pre>
     * PUT my_index
     * {
     *   "mappings": {
     *     "user": {
     *       "_meta": {
     *         "class": "MyApp::User",
     *         "version": {
     *           "min": "1.0",
     *           "max": "1.3"
     *         }
     *       }
     *     }
     *   }
     * }
     * </pre>
     * <p>
     * https://www.elastic.co/guide/en/elasticsearch/reference/5.2/mapping-meta-field.html
     */
    String _meta() default "";
}
