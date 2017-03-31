package org.elasticsearch.dsl.parser.query.method;

import org.elasticsearch.dsl.bean.AtomQuery;
import org.elasticsearch.dsl.exception.ElasticSql2DslException;

public abstract class CheckableMethodQueryParser implements MethodQueryParser {

    protected abstract void checkQueryMethod(MethodInvocation invocation) throws ElasticSql2DslException;

    protected abstract AtomQuery parseMethodQueryWithCheck(MethodInvocation invocation) throws ElasticSql2DslException;

    @Override
    public AtomQuery parseAtomMethodQuery(MethodInvocation invocation) throws ElasticSql2DslException {
        checkQueryMethod(invocation);
        return parseMethodQueryWithCheck(invocation);
    }
}
