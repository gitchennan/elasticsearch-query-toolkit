package org.es.sql.dsl.parser.query.method;

import org.es.sql.dsl.bean.AtomQuery;
import org.es.sql.dsl.exception.ElasticSql2DslException;
import org.es.sql.dsl.helper.ElasticSqlMethodInvokeHelper;

public abstract class CheckableMethodQueryParser implements MethodQueryParser {

    protected abstract void checkMethodInvokeArgs(MethodInvocation invocation) throws ElasticSql2DslException;

    protected abstract AtomQuery parseMethodQueryWithCheck(MethodInvocation invocation) throws ElasticSql2DslException;

    @Override
    public boolean isMatchMethodInvocation(MethodInvocation invocation) {
        return ElasticSqlMethodInvokeHelper.isMethodOf(defineMethodNames(), invocation.getMethodName());
    }

    @Override
    public AtomQuery parseAtomMethodQuery(MethodInvocation invocation) throws ElasticSql2DslException {
        if (!isMatchMethodInvocation(invocation)) {
            throw new ElasticSql2DslException(
                    String.format("[syntax error] Expected method name is one of [%s],but get [%s]",
                            defineMethodNames(), invocation.getMethodName()));
        }
        checkMethodInvokeArgs(invocation);
        return parseMethodQueryWithCheck(invocation);
    }
}
