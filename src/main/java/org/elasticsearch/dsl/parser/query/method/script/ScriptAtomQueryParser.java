package org.elasticsearch.dsl.parser.query.method.script;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.dsl.bean.AtomQuery;
import org.elasticsearch.dsl.exception.ElasticSql2DslException;
import org.elasticsearch.dsl.helper.ElasticSqlArgTransferHelper;
import org.elasticsearch.dsl.helper.ElasticSqlMethodInvokeHelper;
import org.elasticsearch.dsl.listener.ParseActionListener;
import org.elasticsearch.dsl.parser.query.method.AbstractAtomMethodQueryParser;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.Script;

import java.util.List;

public class ScriptAtomQueryParser extends AbstractAtomMethodQueryParser {

    private static List<String> SCRIPT_METHOD = ImmutableList.of("script_query", "scriptQuery");

    public ScriptAtomQueryParser(ParseActionListener parseActionListener) {
        super(parseActionListener);
    }

    public static Boolean isScriptAtomQuery(SQLMethodInvokeExpr methodQueryExpr) {
        return ElasticSqlMethodInvokeHelper.isMethodOf(SCRIPT_METHOD, methodQueryExpr.getMethodName());
    }

    @Override
    protected void checkQueryMethod(SQLMethodInvokeExpr methodQueryExpr, String queryAs, Object[] sqlArgs) {
        if (Boolean.FALSE == isScriptAtomQuery(methodQueryExpr)) {
            throw new ElasticSql2DslException(String.format("[syntax error] Expected script query method name is [script],but get [%s]", methodQueryExpr.getMethodName()));
        }

        int paramCount = methodQueryExpr.getParameters().size();
        if (paramCount != 1) {
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
        return new AtomQuery(QueryBuilders.scriptQuery(new Script(script)));
    }
}
