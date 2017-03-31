package org.elasticsearch.dsl.parser.query.method.term;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.google.common.collect.ImmutableList;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.dsl.exception.ElasticSql2DslException;
import org.elasticsearch.dsl.helper.ElasticSqlArgTransferHelper;
import org.elasticsearch.dsl.helper.ElasticSqlMethodInvokeHelper;
import org.elasticsearch.dsl.listener.ParseActionListener;
import org.elasticsearch.dsl.parser.query.method.AbstractFieldSpecificMethodQueryParser;
import org.elasticsearch.dsl.parser.query.method.MethodInvocation;
import org.elasticsearch.index.query.FuzzyQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.List;
import java.util.Map;

public class FuzzyAtomQueryParser extends AbstractFieldSpecificMethodQueryParser {

    private static List<String> FUZZY_QUERY_METHOD = ImmutableList.of("fuzzy", "fuzzy_query", "fuzzyQuery");

    public FuzzyAtomQueryParser(ParseActionListener parseActionListener) {
        super(parseActionListener);
    }

    @Override
    public boolean isMatchMethodInvocation(MethodInvocation invocation) {
        return ElasticSqlMethodInvokeHelper.isMethodOf(FUZZY_QUERY_METHOD, invocation.getMatchQueryExpr().getMethodName());
    }

    @Override
    protected void checkQueryMethod(MethodInvocation invocation) {

        if (Boolean.FALSE == isMatchMethodInvocation(invocation)) {
            throw new ElasticSql2DslException(
                    String.format("[syntax error] Expected fuzzy query method name is [fuzzy],but get [%s]",
                            invocation.getMatchQueryExpr().getMethodName()));
        }

        SQLMethodInvokeExpr methodQueryExpr = invocation.getMatchQueryExpr();

        int paramCount = methodQueryExpr.getParameters().size();
        if (paramCount != 2 && paramCount != 3) {
            throw new ElasticSql2DslException(String.format("[syntax error] There's no %s args method: fuzzy", paramCount));
        }

        SQLExpr textExpr = methodQueryExpr.getParameters().get(1);

        String text = ElasticSqlArgTransferHelper.transferSqlArg(textExpr, invocation.getSqlArgs(), false).toString();
        if (StringUtils.isEmpty(text)) {
            throw new ElasticSql2DslException("[syntax error] Fuzzy text can not be blank!");
        }
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

        FuzzyQueryBuilder fuzzyQuery = QueryBuilders.fuzzyQuery(fieldName, text);
        setExtraMatchQueryParam(fuzzyQuery, extraParams);
        return fuzzyQuery;
    }

    @Override
    protected SQLExpr getFieldExpr(MethodInvocation invocation) {
        return invocation.getMatchQueryExpr().getParameters().get(0);
    }

    private void setExtraMatchQueryParam(FuzzyQueryBuilder fuzzyQuery, Map<String, String> extraParamMap) {
        if (MapUtils.isEmpty(extraParamMap)) {
            return;
        }
        if (extraParamMap.containsKey("boost")) {
            String val = extraParamMap.get("boost");
            fuzzyQuery.boost(Float.valueOf(val));
        }
        if (extraParamMap.containsKey("transpositions")) {
            String val = extraParamMap.get("transpositions");
            fuzzyQuery.transpositions(Boolean.parseBoolean(val));
        }
        if (extraParamMap.containsKey("prefix_length")) {
            String val = extraParamMap.get("prefix_length");
            fuzzyQuery.prefixLength(Integer.valueOf(val));
        }
        if (extraParamMap.containsKey("max_expansions")) {
            String val = extraParamMap.get("max_expansions");
            fuzzyQuery.maxExpansions(Integer.valueOf(val));
        }
        if (extraParamMap.containsKey("rewrite")) {
            String val = extraParamMap.get("rewrite");
            fuzzyQuery.rewrite(val);
        }

        if (extraParamMap.containsKey("fuzziness")) {
            String val = extraParamMap.get("fuzziness");

            if ("ZERO".equalsIgnoreCase(val) || "0".equals(val)) {
                fuzzyQuery.fuzziness(Fuzziness.ZERO);
            }
            if ("ONE".equalsIgnoreCase(val) || "1".equals(val)) {
                fuzzyQuery.fuzziness(Fuzziness.ONE);
            }

            if ("TWO".equalsIgnoreCase(val) || "2".equals(val)) {
                fuzzyQuery.fuzziness(Fuzziness.TWO);
            }

            if ("AUTO".equalsIgnoreCase(val)) {
                fuzzyQuery.fuzziness(Fuzziness.AUTO);
            }
        }
    }
}
