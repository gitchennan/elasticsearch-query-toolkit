package org.elasticsearch.dsl.parser.query.method.script;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.elasticsearch.dsl.bean.AtomQuery;
import org.elasticsearch.dsl.exception.ElasticSql2DslException;
import org.elasticsearch.dsl.helper.ElasticSqlArgTransferHelper;
import org.elasticsearch.dsl.helper.ElasticSqlMethodInvokeHelper;
import org.elasticsearch.dsl.listener.ParseActionListener;
import org.elasticsearch.dsl.parser.query.method.AbstractAtomMethodQueryParser;
import org.elasticsearch.dsl.parser.query.method.MethodInvocation;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;

import java.util.List;
import java.util.Map;

public class ScriptAtomQueryParser extends AbstractAtomMethodQueryParser {

    private static List<String> SCRIPT_METHOD = ImmutableList.of("script_query", "scriptQuery");

    public ScriptAtomQueryParser(ParseActionListener parseActionListener) {
        super(parseActionListener);
    }

    @Override
    public boolean isMatchMethodInvocation(MethodInvocation invocation) {
        return ElasticSqlMethodInvokeHelper.isMethodOf(SCRIPT_METHOD, invocation.getMatchQueryExpr().getMethodName());
    }

    @Override
    protected void checkQueryMethod(SQLMethodInvokeExpr methodQueryExpr, String queryAs, Object[] sqlArgs) {
        if (Boolean.FALSE == isMatchMethodInvocation(new MethodInvocation(methodQueryExpr, queryAs, sqlArgs))) {
            throw new ElasticSql2DslException(String.format("[syntax error] Expected script query method name is [script],but get [%s]", methodQueryExpr.getMethodName()));
        }

        int paramCount = methodQueryExpr.getParameters().size();
        if (paramCount != 1 && paramCount != 2) {
            throw new ElasticSql2DslException(String.format("[syntax error] There's no %s args method: fuzzy", paramCount));
        }

        SQLExpr scriptExpr = methodQueryExpr.getParameters().get(0);
        String text = ElasticSqlArgTransferHelper.transferSqlArg(scriptExpr, sqlArgs, false).toString();
        if (StringUtils.isEmpty(text)) {
            throw new ElasticSql2DslException("[syntax error] script can not be blank!");
        }
    }

    @Override
    protected AtomQuery parseMethodQueryExpr(SQLMethodInvokeExpr methodQueryExpr, String queryAs, Object[] sqlArgs) {
        String script = (String) ElasticSqlArgTransferHelper.transferSqlArg(methodQueryExpr.getParameters().get(0), sqlArgs, false);
        int paramCount = methodQueryExpr.getParameters().size();
        if (paramCount == 2) {
            String scriptParams = (String) ElasticSqlArgTransferHelper.transferSqlArg(methodQueryExpr.getParameters().get(1), sqlArgs, false);
            Map<String, Object> scriptParamMap = buildScriptParamMap(scriptParams);
            return new AtomQuery(QueryBuilders.scriptQuery(new Script(script, ScriptService.ScriptType.INLINE, null, scriptParamMap)));
        }
        return new AtomQuery(QueryBuilders.scriptQuery(new Script(script)));
    }

    private Map<String, Object> buildScriptParamMap(String scriptStrParams) {
        Map<String, String> scriptStrParamMap = buildExtraMethodQueryParamsMap(scriptStrParams);
        return Maps.transformEntries(scriptStrParamMap, new Maps.EntryTransformer<String, String, Object>() {
            @Override
            public Object transformEntry(String key, String value) {
                return NumberUtils.isNumber(value) ? NumberUtils.createNumber(value) : value;
            }
        });
    }
}
