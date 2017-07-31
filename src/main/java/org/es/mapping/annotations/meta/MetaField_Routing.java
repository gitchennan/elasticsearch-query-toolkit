package org.es.mapping.annotations.meta;

import java.lang.annotation.*;

@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface MetaField_Routing {
    /**
     * A document is routed to a particular shard in an index using the following formula:
     * <pre>shard_num = hash(_routing) % num_primary_shards</pre>
     * <p>
     * Forgetting the routing value can lead to a document being indexed on more than one shard.
     * As a safeguard, the _routing field can be configured to make a custom routing value required for all CRUD operations
     * <p>
     * default set to false to disable custom routing value
     */
    boolean required() default false;
}
