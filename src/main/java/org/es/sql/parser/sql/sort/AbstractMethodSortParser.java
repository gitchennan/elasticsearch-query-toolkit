package org.es.sql.parser.sql.sort;

import org.apache.commons.lang.StringUtils;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.es.sql.exception.ElasticSql2DslException;
import org.es.sql.helper.ElasticSqlMethodInvokeHelper;
import org.es.sql.parser.query.method.MethodInvocation;
import org.es.sql.parser.query.method.expr.AbstractParameterizedMethodExpression;

import java.util.Map;

public abstract class AbstractMethodSortParser extends AbstractParameterizedMethodExpression implements MethodSortParser {

    protected abstract SortBuilder parseMethodSortBuilder(
            MethodInvocation invocation, SortOrder order, Map<String, Object> extraParamMap) throws ElasticSql2DslException;

    @Override
    protected String defineExtraParamString(MethodInvocation invocation) {
        return StringUtils.EMPTY;
    }

    @Override
    public void checkMethodInvocation(MethodInvocation invocation) throws ElasticSql2DslException {

    }

    @Override
    public boolean isMatchMethodInvocation(MethodInvocation invocation) {
        return ElasticSqlMethodInvokeHelper.isMethodOf(defineMethodNames(), invocation.getMethodName());
    }

    @Override
    public SortBuilder parseMethodSortBuilder(MethodInvocation invocation, SortOrder order) throws ElasticSql2DslException {
        if (!isMatchMethodInvocation(invocation)) {
            throw new ElasticSql2DslException(
                    String.format("[syntax error] Expected method name is one of [%s],but get [%s]",
                            defineMethodNames(), invocation.getMethodName()));
        }
        checkMethodInvocation(invocation);

        Map<String, Object> extraParamMap = generateRawTypeParameterMap(invocation);
        return parseMethodSortBuilder(invocation, order, extraParamMap);
    }
}
