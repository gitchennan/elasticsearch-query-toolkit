package org.es.sql.dsl.parser.query.method.term;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.es.sql.dsl.exception.ElasticSql2DslException;
import org.es.sql.dsl.listener.ParseActionListener;
import org.es.sql.dsl.parser.query.method.AbstractFieldSpecificMethodQueryParser;
import org.es.sql.dsl.parser.query.method.MethodInvocation;

import java.util.List;
import java.util.Map;

public class PrefixAtomQueryParser extends AbstractFieldSpecificMethodQueryParser {

    private static List<String> PREFIX_QUERY_METHOD = ImmutableList.of("prefix", "prefix_query", "prefixQuery");

    public PrefixAtomQueryParser(ParseActionListener parseActionListener) {
        super(parseActionListener);
    }

    @Override
    public List<String> defineMethodNames() {
        return PREFIX_QUERY_METHOD;
    }

    @Override
    protected SQLExpr defineFieldExpr(MethodInvocation invocation) {
        return invocation.getParameter(0);
    }

    @Override
    protected void checkMethodInvokeArgs(MethodInvocation invocation) {
        if (invocation.getParameterCount() != 2) {
            throw new ElasticSql2DslException(
                    String.format("[syntax error] There's no %s args method named [%s].",
                            invocation.getParameterCount(), invocation.getMethodName()));
        }

        String text = invocation.getParameterAsString(1);
        if (StringUtils.isEmpty(text)) {
            throw new ElasticSql2DslException("[syntax error] Prefix search text can not be blank!");
        }
    }

    @Override
    protected FilterBuilder buildQuery(MethodInvocation invocation, String fieldName, Map<String, String> extraParams) {
        String text = invocation.getParameterAsString(1);
        return FilterBuilders.prefixFilter(fieldName, text);
    }
}
