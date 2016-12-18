package org.elasticsearch.dsl.parser;

import org.elasticsearch.dsl.ElasticDslContext;

@FunctionalInterface
public interface ElasticSqlParser {
    void parse(ElasticDslContext dslContext);
}
