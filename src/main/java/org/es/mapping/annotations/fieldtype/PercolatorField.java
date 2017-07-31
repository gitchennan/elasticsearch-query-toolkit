package org.es.mapping.annotations.fieldtype;

import java.lang.annotation.*;

/**
 * The percolator field type parses a json structure into a native query and stores that query,
 * so that the percolate query can use it to match provided documents.
 * <p>
 * Any field that contains a json object can be configured to be a percolator field.
 * The percolator field type has no settings. Just configuring the percolator field type is
 * sufficient to instruct Elasticsearch to treat a field as a query.
 * <p>
 * https://www.elastic.co/guide/en/elasticsearch/reference/current/percolator.html
 */
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD})
public @interface PercolatorField {

}