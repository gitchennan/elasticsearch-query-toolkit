package org.es.mapping.annotations.fieldtype;



import org.es.mapping.annotations.enums.IndexOptions;
import org.es.mapping.annotations.enums.SimilarityAlgorithm;
import org.es.mapping.annotations.enums.StringType;
import org.es.mapping.annotations.enums.TermVector;

import java.lang.annotation.*;

@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD})
public @interface StringField {
    /**
     * type of string
     * <p>
     * {@link StringType}
     */
    StringType type();

    /**
     * Mapping field-level query time boosting. Accepts a floating point number, defaults to 1.0.
     */
    float boost() default 1.0f;

    /**
     * The copy_to parameter allows you to create custom _all fields. In other words,
     * the values of multiple fields can be copied into a group field,
     * which can then be queried as a single field. For instance,
     * the first_name and last_name fields can be copied to the full_name field
     */
    String[] copy_to() default {};

    /**
     * Should the field be stored on disk in a column-stride fashion,
     * so that it can later be used for sorting, aggregations, or scripting?
     * Accepts true (default) or false.
     */
    boolean doc_values() default true;

    /**
     * Should global ordinals be loaded eagerly on refresh? Accepts true or false (default).
     * Enabling this is a good idea on fields that are frequently used for terms aggregations.
     */
    boolean eager_global_ordinals() default false;


    /**
     * Do not index any string longer than this value.
     * Defaults to 2147483647 so that all values would be accepted.
     */
    int ignore_above() default 2147483647;

    /**
     * Whether or not the field value should be included in the _all field?
     * Accepts true or false. Defaults to false if index is set to no,
     * or if a parent object field sets include_in_all to false. Otherwise defaults to true.
     */
    boolean include_in_all() default true;

    /**
     * Should the field be searchable? Accepts true (default) or false.
     */
    boolean index() default true;

    /**
     * What information should be stored in the index, for scoring purposes.
     * Defaults to docs but can also be set to freqs to take term frequency into account when computing scores.
     */
    IndexOptions index_options() default IndexOptions.Default;

    /**
     * Accepts a string value which is substituted for any explicit null values.
     * Defaults to null, which means the field is treated as missing.
     */
    String null_value() default "";

    /**
     * Whether the field value should be stored and retrievable separately from the _source field.
     * Accepts true or false (default).
     */
    boolean store() default false;

    /**
     * Which scoring algorithm or similarity should be used. Defaults to BM25.
     */
    SimilarityAlgorithm similarity() default SimilarityAlgorithm.Default;


    /**
     * How to pre-process the keyword prior to indexing. Defaults to null, meaning the keyword is kept as-is.
     *
     * NOTE: This functionality is experimental and may be changed or removed completely in a future release.
     * Elastic will take a best effort approach to fix any issues,
     * but experimental features are not subject to the support SLA of official GA features.
     */
    // normalizer, not support


    /**
     * *********************************    NEXT FOR FULL TEXT   *********************************
     */


    /**
     * The analyzer which should be used for analyzed string fields, both at index-time and at
     * search-time (unless overridden by the search_analyzer). Defaults to the default index analyzer,
     * or the standard analyzer.
     */
    String analyzer() default "";

    /**
     * The analyzer that should be used at search time on analyzed fields. Defaults to the analyzer setting.
     */
    String search_analyzer() default "";

    /**
     * The analyzer that should be used at search time when a phrase is encountered. Defaults to the search_analyzer setting.
     */
    String search_quote_analyzer() default "";

    /**
     * The number of fake term position which should be inserted between each element of an array of strings.
     * Defaults to the position_increment_gap configured on the analyzer which defaults to 100. 100 was chosen
     * because it prevents phrase queries with reasonably large slops (less than 100) from matching terms across field values.
     */
    int position_increment_gap() default 100;

    /**
     * Whether field-length should be taken into account when scoring queries. Accepts true (default) or false.
     */
    boolean norms() default true;

    /**
     * Can the field use in-memory fielddata for sorting, aggregations, or scripting? Accepts true or false (default).
     */
    Fielddata fielddata() default @Fielddata;

    /**
     * Whether term vectors should be stored for an analyzed field. Defaults to no.
     */
    TermVector term_vector() default TermVector.No;
}


