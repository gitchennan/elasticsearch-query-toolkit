package org.es.mapping.annotations.fieldtype;

public @interface CompletionContext {

    String name();

    String type() default "category";

    String path() default "";
}
