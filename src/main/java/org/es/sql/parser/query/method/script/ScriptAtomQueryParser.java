package org.es.sql.parser.query.method.script;

import com.google.common.collect.ImmutableList;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.Script;
import org.es.sql.bean.AtomQuery;
import org.es.sql.exception.ElasticSql2DslException;
import org.es.sql.parser.query.method.MethodInvocation;
import org.es.sql.parser.query.method.ParameterizedMethodQueryParser;

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
    public void checkMethodInvocation(MethodInvocation invocation) throws ElasticSql2DslException {
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
    protected AtomQuery parseMethodQueryWithExtraParams(MethodInvocation invocation, Map<String, String> extraParamMap) throws ElasticSql2DslException {
        String script = invocation.getParameterAsString(0);

        if (MapUtils.isNotEmpty(extraParamMap)) {
            Map<String, Object> scriptParamMap = generateRawTypeParameterMap(invocation);
            return new AtomQuery(QueryBuilders.scriptQuery(
                    new Script(Script.DEFAULT_SCRIPT_TYPE, Script.DEFAULT_SCRIPT_LANG, script, scriptParamMap))
            );
        }
        return new AtomQuery(QueryBuilders.scriptQuery(new Script(script)));
    }


}
