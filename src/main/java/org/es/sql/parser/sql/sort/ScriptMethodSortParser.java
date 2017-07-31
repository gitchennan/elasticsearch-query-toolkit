package org.es.sql.parser.sql.sort;

import com.google.common.collect.ImmutableList;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.sort.ScriptSortBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.es.sql.exception.ElasticSql2DslException;
import org.es.sql.parser.query.method.MethodInvocation;


import java.util.List;
import java.util.Map;

public class ScriptMethodSortParser extends AbstractMethodSortParser {

    public static final List<String> SCRIPT_SORT_METHOD = ImmutableList.of("script_sort", "scriptSort");

    @Override
    public List<String> defineMethodNames() {
        return SCRIPT_SORT_METHOD;
    }

    @Override
    protected String defineExtraParamString(MethodInvocation invocation) {
        if (invocation.getParameterCount() == 3) {
            return invocation.getParameterAsString(2);
        }
        return StringUtils.EMPTY;
    }

    @Override
    public void checkMethodInvocation(MethodInvocation nvlMethodInvocation) throws ElasticSql2DslException {
        if (!isMatchMethodInvocation(nvlMethodInvocation)) {
            throw new ElasticSql2DslException("[syntax error] Sql sort condition only support script_query method invoke");
        }

        int methodParameterCount = nvlMethodInvocation.getParameterCount();
        if (methodParameterCount != 2 && methodParameterCount != 3) {
            throw new ElasticSql2DslException(String.format("[syntax error] There is no %s args method named script_sort", methodParameterCount));
        }
    }

    @Override
    protected SortBuilder parseMethodSortBuilder(
            MethodInvocation scriptSortMethodInvocation, SortOrder order, Map<String, Object> extraParamMap) throws ElasticSql2DslException {

        String strScript = scriptSortMethodInvocation.getParameterAsString(0);
        String type = scriptSortMethodInvocation.getParameterAsString(1);
        ScriptSortBuilder.ScriptSortType scriptSortType = ScriptSortBuilder.ScriptSortType.fromString(type);

        if (MapUtils.isNotEmpty(extraParamMap)) {
            Map<String, Object> scriptParamMap = generateRawTypeParameterMap(scriptSortMethodInvocation);
            Script scriptObject = new Script(Script.DEFAULT_SCRIPT_TYPE, Script.DEFAULT_SCRIPT_LANG, strScript, scriptParamMap);
            return SortBuilders.scriptSort(scriptObject, scriptSortType).order(order);
        }

        return SortBuilders.scriptSort(new Script(strScript), scriptSortType).order(order);
    }
}
