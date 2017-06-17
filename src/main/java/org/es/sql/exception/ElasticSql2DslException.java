package org.es.sql.exception;

import com.alibaba.druid.sql.parser.ParserException;

public class ElasticSql2DslException extends RuntimeException {
    public ElasticSql2DslException(String message) {
        super(message);
    }

    public ElasticSql2DslException(ParserException ex) {
        super(ex);
    }
}
