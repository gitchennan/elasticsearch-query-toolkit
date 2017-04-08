package org.es.sql.dsl.parser.query.method.term;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.es.sql.dsl.exception.ElasticSql2DslException;
import org.es.sql.dsl.listener.ParseActionListener;
import org.es.sql.dsl.parser.query.method.AbstractFieldSpecificMethodQueryParser;
import org.es.sql.dsl.parser.query.method.MethodInvocation;

import java.util.List;
import java.util.Map;

public class TermsAtomQueryParser extends AbstractFieldSpecificMethodQueryParser {

    private static List<String> TERMS_QUERY_METHOD = ImmutableList.of("terms", "terms_query", "termsQuery");

    public TermsAtomQueryParser(ParseActionListener parseActionListener) {
        super(parseActionListener);
    }

    @Override
    public List<String> defineMethodNames() {
        return TERMS_QUERY_METHOD;
    }

    @Override
    protected SQLExpr defineFieldExpr(MethodInvocation invocation) {
        return invocation.getParameter(0);
    }

    @Override
    protected String defineExtraParamString(MethodInvocation invocation) {
        String extraParamString = invocation.getLastParameterAsString();
        if (isExtraParamsString(extraParamString)) {
            return extraParamString;
        }
        return StringUtils.EMPTY;
    }

    @Override
    protected void checkMethodInvokeArgs(MethodInvocation invocation) {
        if (invocation.getParameterCount() <= 1) {
            throw new ElasticSql2DslException(
                    String.format("[syntax error] There's no %s args method named [%s].",
                            invocation.getParameterCount(), invocation.getMethodName()));
        }

        int paramCount = invocation.getParameterCount();

        for (int idx = 1; idx < paramCount - 1; idx++) {
            String text = invocation.getParameterAsString(idx);
            if (StringUtils.isEmpty(text)) {
                throw new ElasticSql2DslException("[syntax error] Terms text can not be blank!");
            }
        }
    }

    @Override
    protected FilterBuilder buildQuery(MethodInvocation invocation, String fieldName, Map<String, String> extraParams) {
        int paramCount = invocation.getParameterCount();

        List<String> termTextList = Lists.newArrayList();
        for (int idx = 1; idx < paramCount - 1; idx++) {
            String text = invocation.getParameterAsString(idx);
            termTextList.add(text);
        }

        String lastParamText = invocation.getLastParameterAsString();
        if (!isExtraParamsString(lastParamText)) {
            termTextList.add(lastParamText);
        }

        return FilterBuilders.termsFilter(fieldName, termTextList);
    }
}
