package org.es.mapping.annotations.fieldtype;

import java.lang.annotation.*;

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD})
@Documented
@Inherited
public @interface CompletionField {

    /**
     * The index analyzer to use, defaults to simple. In case you are wondering why we did
     * not opt for the standard analyzer: We try to have easy to understand behaviour here,
     * and if you index the field content At the Drive-in,
     * you will not get any suggestions for a, nor for d (the first non stopword).
     */
    String analyzer() default "simple";

    /**
     * The search analyzer to use, defaults to value of analyzer.
     */
    String search_analyzer() default "simple";

    /**
     * Preserves the separators, defaults to true.
     * If disabled, you could find a field starting with Foo Fighters,if you suggest for foof.
     */
    boolean preserve_separators() default true;

    /**
     * Enables position increments, defaults to true. If disabled and using stopwords analyzer,
     * you could get a field starting with The Beatles,
     * if you suggest for b. Note: You could also achieve this by indexing two inputs,
     * Beatles and The Beatles, no need to change a simple analyzer, if you are able to enrich your data.
     */
    boolean preserve_position_increments() default true;

    /**
     * Limits the length of a single input, defaults to 50 UTF-16 code points.
     * This limit is only used at index time to reduce the total number of characters per input string
     * in order to prevent massive inputs from bloating the underlying datastructure.
     * Most usecases won’t be influenced by the default value since prefix completions seldom grow beyond
     * prefixes longer than a handful of characters.
     */
    int max_input_length() default 50;

    /**
     * To achieve suggestion filtering and/or boosting, you can add context mappings while configuring a completion field.
     * You can define multiple context mappings for a completion field. Every context mapping has a unique name and a type.
     * There are two types: category and geo.
     * Context mappings are configured under the contexts parameter in the field mapping.
     */
    CompletionContext[] contexts() default {};
}
