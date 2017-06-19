package org.es.sql.parser.query.method.expr;

import com.google.common.collect.Maps;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.es.sql.exception.ElasticSql2DslException;
import org.es.sql.parser.query.method.MethodInvocation;

import java.util.Collections;
import java.util.Map;

public abstract class AbstractParameterizedMethodExpression implements ParameterizedMethodExpression {

    protected static final String COMMA = ",";

    protected static final String COLON = ":";

    protected abstract String defineExtraParamString(MethodInvocation invocation);

    public Map<String, String> generateParameterMap(MethodInvocation invocation) {
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

    public Map<String, Object> generateRawTypeParameterMap(MethodInvocation invocation) {
        Map<String, String> extraParamMap = generateParameterMap(invocation);
        if (MapUtils.isNotEmpty(extraParamMap)) {
            return Maps.transformEntries(extraParamMap, new Maps.EntryTransformer<String, String, Object>() {
                @Override
                public Object transformEntry(String key, String value) {
                    return NumberUtils.isNumber(value) ? NumberUtils.createNumber(value) : value;
                }
            });
        }

        return Collections.emptyMap();
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
