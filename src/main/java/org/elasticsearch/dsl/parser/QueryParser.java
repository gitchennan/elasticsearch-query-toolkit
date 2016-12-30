package org.elasticsearch.dsl.parser;

import org.elasticsearch.dsl.ElasticDslContext;

@FunctionalInterface
public interface QueryParser {
    void parse(ElasticDslContext dslContext);
}
