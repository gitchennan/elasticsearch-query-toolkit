package org.es.mapping.annotations.meta;

import java.lang.annotation.*;

/**
 * A parent-child relationship can be established between documents
 * in the same index by making one mapping type the parent of another
 * <p/>
 * Parent-child restrictions
 * 1. The parent and child types must be different parent-child relationships
 * cannot be established between documents of the same type.
 * <p/>
 * 2. The _parent.type setting can only point to a type that doesn't exist yet.
 * This means that a type cannot become a parent type after it has been created.
 * <p/>
 * 3. Parent and child documents must be indexed on the same shard.
 * The parent ID is used as the routing value for the child, to ensure that the child is indexed on the same shard as the parent.
 * This means that the same parent value needs to be provided when getting, deleting, or updating a child document.
 * <p/>
 * https://www.elastic.co/guide/en/elasticsearch/reference/5.2/mapping-parent-field.html
 *
 * @author chennan
 */
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface MetaField_Parent {

    /**
     * parent class
     */
    Class<?>[] parentClass();

    /**
     * Parent-child uses global ordinals to speed up joins. Global ordinals need to be rebuilt after any change to a shard.
     * The more parent id values are stored in a shard, the longer it takes to rebuild the global ordinals for the _parent field.
     * <p/>
     * Global ordinals, by default, are built eagerly: if the index has changed, global ordinals for the _parent field
     * will be rebuilt as part of the refresh. This can add significant time the refresh. However most of the times this is the right trade-off,
     * otherwise global ordinals are rebuilt when the first parent-child query or aggregation is used.
     * This can introduce a significant latency spike for your users and usually this is worse as multiple global ordinals for the _parent
     * field may be attempt rebuilt within a single refresh interval when many writes are occurring.
     * <p/>
     * NOTE: When the parent/child is used infrequently and writes occur frequently it may make sense to disable eager loading
     * <p/>
     * default set to true
     */
    boolean eager_global_ordinals() default true;
}
