package org.elasticsearch.dsl.parser.query.method.term;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.dsl.exception.ElasticSql2DslException;
import org.elasticsearch.dsl.helper.ElasticSqlArgTransferHelper;
import org.elasticsearch.dsl.helper.ElasticSqlMethodInvokeHelper;
import org.elasticsearch.dsl.listener.ParseActionListener;
import org.elasticsearch.dsl.parser.query.method.AbstractFieldSpecificMethodQueryParser;
import org.elasticsearch.dsl.parser.query.method.MethodInvocation;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RegexpFlag;
import org.elasticsearch.index.query.RegexpQueryBuilder;

import java.util.List;
import java.util.Map;

public class RegexpAtomQueryParser extends AbstractFieldSpecificMethodQueryParser {

    private static List<String> REGEXP_QUERY_METHOD = ImmutableList.of("regexp", "regexp_query", "regexpQuery");

    public RegexpAtomQueryParser(ParseActionListener parseActionListener) {
        super(parseActionListener);
    }

    @Override
    public boolean isMatchMethodInvocation(MethodInvocation invocation) {
        return ElasticSqlMethodInvokeHelper.isMethodOf(REGEXP_QUERY_METHOD, invocation.getMatchQueryExpr().getMethodName());
    }

    @Override
    protected String getExtraParamString(MethodInvocation invocation) {
        SQLMethodInvokeExpr methodInvokeExpr = invocation.getMatchQueryExpr();
        if (methodInvokeExpr.getParameters().size() == 3) {
            SQLExpr extraParamExpr = methodInvokeExpr.getParameters().get(2);
            Object[] sqlArgs = invocation.getSqlArgs();
            return ElasticSqlArgTransferHelper.transferSqlArg(extraParamExpr, sqlArgs, false).toString();
        }
        return StringUtils.EMPTY;
    }

    @Override
    protected QueryBuilder buildQuery(MethodInvocation invocation, String fieldName, Map<String, String> extraParams) {
        SQLMethodInvokeExpr methodInvokeExpr = invocation.getMatchQueryExpr();

        SQLExpr textExpr = methodInvokeExpr.getParameters().get(1);
        Object text = ElasticSqlArgTransferHelper.transferSqlArg(textExpr, invocation.getSqlArgs(), false);

        RegexpQueryBuilder regexpQuery = QueryBuilders.regexpQuery(fieldName, text.toString());
        setExtraMatchQueryParam(regexpQuery, extraParams);
        return regexpQuery;
    }

    @Override
    protected SQLExpr getFieldExpr(MethodInvocation invocation) {
        return invocation.getMatchQueryExpr().getParameters().get(0);
    }

    @Override
    protected void checkQueryMethod(MethodInvocation invocation) throws ElasticSql2DslException {
        if (Boolean.FALSE == isMatchMethodInvocation(invocation)) {
            throw new ElasticSql2DslException(
                    String.format("[syntax error] Expected regexp query method name is [regexp],but get [%s]",
                            invocation.getMatchQueryExpr().getMethodName()));
        }

        SQLMethodInvokeExpr methodQueryExpr = invocation.getMatchQueryExpr();

        int paramCount = methodQueryExpr.getParameters().size();
        if (paramCount != 2 && paramCount != 3) {
            throw new ElasticSql2DslException(String.format("[syntax error] There's no %s args method: regexp", paramCount));
        }

        SQLExpr textExpr = methodQueryExpr.getParameters().get(1);

        String text = ElasticSqlArgTransferHelper.transferSqlArg(textExpr, invocation.getSqlArgs(), false).toString();
        if (StringUtils.isEmpty(text)) {
            throw new ElasticSql2DslException("[syntax error] Regexp text can not be blank!");
        }
    }

    private void setExtraMatchQueryParam(RegexpQueryBuilder regexpQuery, Map<String, String> extraParamMap) {
        if (MapUtils.isEmpty(extraParamMap)) {
            return;
        }
        if (extraParamMap.containsKey("boost")) {
            String val = extraParamMap.get("boost");
            regexpQuery.boost(Float.valueOf(val));
        }
        if (extraParamMap.containsKey("rewrite")) {
            String val = extraParamMap.get("rewrite");
            regexpQuery.rewrite(val);
        }
        if (extraParamMap.containsKey("max_determinized_states")) {
            String val = extraParamMap.get("max_determinized_states");
            regexpQuery.maxDeterminizedStates(Integer.valueOf(val));
        }
        if (extraParamMap.containsKey("flags")) {
            String[] flags = extraParamMap.get("flags").split("\\|");
            List<RegexpFlag> flagList = Lists.newLinkedList();
            for (String flag : flags) {
                flagList.add(RegexpFlag.valueOf(flag.toUpperCase()));
            }
            regexpQuery.flags(flagList.toArray(new RegexpFlag[flagList.size()]));
        }
        if (extraParamMap.containsKey("flags_value")) {
            String[] flags = extraParamMap.get("flags_value").split("\\|");
            List<RegexpFlag> flagList = Lists.newLinkedList();
            for (String flag : flags) {
                flagList.add(RegexpFlag.valueOf(flag.toUpperCase()));
            }
            regexpQuery.flags(flagList.toArray(new RegexpFlag[flagList.size()]));
        }
    }
}
