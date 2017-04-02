package org.es.sql.dsl.parser.sql;

import org.es.sql.dsl.bean.ElasticDslContext;

@FunctionalInterface
public interface QueryParser {
    void parse(ElasticDslContext dslContext);
}
