package org.es.sql.parser.query.method.term;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RegexpFlag;
import org.elasticsearch.index.query.RegexpQueryBuilder;

import org.es.sql.exception.ElasticSql2DslException;
import org.es.sql.listener.ParseActionListener;
import org.es.sql.parser.query.method.AbstractFieldSpecificMethodQueryParser;
import org.es.sql.parser.query.method.MethodInvocation;

import java.util.List;
import java.util.Map;

public class RegexpAtomQueryParser extends AbstractFieldSpecificMethodQueryParser {

    private static List<String> REGEXP_QUERY_METHOD = ImmutableList.of("regexp", "regexp_query", "regexpQuery");

    public RegexpAtomQueryParser(ParseActionListener parseActionListener) {
        super(parseActionListener);
    }

    @Override
    public List<String> defineMethodNames() {
        return REGEXP_QUERY_METHOD;
    }

    @Override
    public SQLExpr defineFieldExpr(MethodInvocation invocation) {
        return invocation.getParameter(0);
    }

    @Override
    protected String defineExtraParamString(MethodInvocation invocation) {
        int extraParamIdx = 2;

        return (invocation.getParameterCount() == extraParamIdx + 1)
                ? invocation.getParameterAsString(extraParamIdx) : StringUtils.EMPTY;
    }

    @Override
    public void checkMethodInvocation(MethodInvocation invocation) throws ElasticSql2DslException {
        if (invocation.getParameterCount() != 2 && invocation.getParameterCount() != 3) {
            throw new ElasticSql2DslException(
                    String.format("[syntax error] There's no %s args method named [%s].",
                            invocation.getParameterCount(), invocation.getMethodName()));
        }

        String text = invocation.getParameterAsString(1);
        if (StringUtils.isEmpty(text)) {
            throw new ElasticSql2DslException("[syntax error] Regexp search text can not be blank!");
        }

        if (invocation.getParameterCount() == 3) {
            String extraParamString = defineExtraParamString(invocation);
            if (StringUtils.isEmpty(extraParamString)) {
                throw new ElasticSql2DslException("[syntax error] The extra param of regexp method can not be blank");
            }
        }
    }

    @Override
    protected QueryBuilder buildQuery(MethodInvocation invocation, String fieldName, Map<String, String> extraParams) {
        String text = invocation.getParameterAsString(1);
        RegexpQueryBuilder regexpQuery = QueryBuilders.regexpQuery(fieldName, text);

        setExtraMatchQueryParam(regexpQuery, extraParams);
        return regexpQuery;
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
