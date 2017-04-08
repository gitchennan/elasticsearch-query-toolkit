package org.es.sql.dsl.parser.query.method.script;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.elasticsearch.index.query.FilterBuilders;
import org.es.sql.dsl.bean.AtomFilter;
import org.es.sql.dsl.exception.ElasticSql2DslException;
import org.es.sql.dsl.parser.query.method.MethodInvocation;
import org.es.sql.dsl.parser.query.method.ParameterizedMethodQueryParser;

import java.util.List;
import java.util.Map;

public class ScriptAtomQueryParser extends ParameterizedMethodQueryParser {

    private static List<String> SCRIPT_METHOD = ImmutableList.of("script_query", "scriptQuery");

    @Override
    public List<String> defineMethodNames() {
        return SCRIPT_METHOD;
    }

    @Override
    protected String defineExtraParamString(MethodInvocation invocation) {
        int extraParamIdx = 1;

        return (invocation.getParameterCount() == extraParamIdx + 1)
                ? invocation.getParameterAsString(extraParamIdx) : StringUtils.EMPTY;
    }

    @Override
    protected void checkMethodInvokeArgs(MethodInvocation invocation) throws ElasticSql2DslException {
        if (invocation.getParameterCount() != 1 && invocation.getParameterCount() != 2) {
            throw new ElasticSql2DslException(
                    String.format("[syntax error] There's no %s args method named [%s].",
                            invocation.getParameterCount(), invocation.getMethodName()));
        }

        String script = invocation.getParameterAsString(0);
        if (StringUtils.isEmpty(script)) {
            throw new ElasticSql2DslException("[syntax error] Script can not be blank!");
        }
    }

    @Override
    protected AtomFilter parseMethodQueryWithExtraParams(MethodInvocation invocation, Map<String, String> extraParamMap) throws ElasticSql2DslException {
        String script = invocation.getParameterAsString(0);

        if (MapUtils.isNotEmpty(extraParamMap)) {
            Map<String, Object> scriptParamMap = Maps.transformEntries(extraParamMap, new Maps.EntryTransformer<String, String, Object>() {
                @Override
                public Object transformEntry(String key, String value) {
                    return NumberUtils.isNumber(value) ? NumberUtils.createNumber(value) : value;
                }
            });
            return new AtomFilter(FilterBuilders.scriptFilter(script).params(scriptParamMap));
        }
        return new AtomFilter(FilterBuilders.scriptFilter(script));
    }
}
