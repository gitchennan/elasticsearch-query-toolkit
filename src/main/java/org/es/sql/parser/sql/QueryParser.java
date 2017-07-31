package org.es.sql.parser.sql;


import org.es.sql.bean.ElasticDslContext;

@FunctionalInterface
public interface QueryParser {
    void parse(ElasticDslContext dslContext);
}
