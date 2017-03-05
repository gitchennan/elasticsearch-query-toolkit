package org.elasticsearch.dsl.parser.syntax.sql;

import org.elasticsearch.dsl.bean.ElasticDslContext;

@FunctionalInterface
public interface QueryParser {
    void parse(ElasticDslContext dslContext);
}
