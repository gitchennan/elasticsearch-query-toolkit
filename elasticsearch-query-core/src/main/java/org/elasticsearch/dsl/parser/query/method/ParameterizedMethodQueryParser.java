package org.elasticsearch.dsl.parser.query.method;

import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.dsl.bean.AtomQuery;
import org.elasticsearch.dsl.exception.ElasticSql2DslException;

import java.util.Collections;
import java.util.Map;

public abstract class ParameterizedMethodQueryParser extends CheckableMethodQueryParser {

    protected static final String COMMA = ",";

    protected static final String COLON = ":";

    protected abstract String defineExtraParamString(MethodInvocation invocation);

    protected abstract AtomQuery parseMethodQueryWithExtraParams(
            MethodInvocation invocation, Map<String, String> extraParamMap) throws ElasticSql2DslException;

    @Override
    protected AtomQuery parseMethodQueryWithCheck(MethodInvocation invocation) {
        Map<String, String> extraParamMap = buildExtraParamMap(invocation);
        return parseMethodQueryWithExtraParams(invocation, extraParamMap);
    }

    private Map<String, String> buildExtraParamMap(MethodInvocation invocation) {
        String extraParamString = defineExtraParamString(invocation);

        if (StringUtils.isBlank(extraParamString)) {
            return Collections.emptyMap();
        }

        Map<String, String> extraParamMap = Maps.newHashMap();
        for (String paramPair : extraParamString.split(COMMA)) {
            String[] paramPairArr = paramPair.split(COLON);
            if (paramPairArr.length == 2) {
                extraParamMap.put(paramPairArr[0].trim(), paramPairArr[1].trim());
            }
            else {
                throw new ElasticSql2DslException("Failed to parse query method extra param string!");
            }
        }
        return extraParamMap;
    }

    protected Boolean isExtraParamsString(String extraParams) {
        if (StringUtils.isBlank(extraParams)) {
            return Boolean.FALSE;
        }
        for (String paramPair : extraParams.split(COMMA)) {
            String[] paramPairArr = paramPair.split(COLON);
            if (paramPairArr.length != 2) {
                return Boolean.FALSE;
            }
        }
        return Boolean.TRUE;
    }
}
