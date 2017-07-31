package org.es.jdbc.es;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.es.jdbc.exception.ResolveSearchResponseException;
import org.es.sql.utils.Constants;

import java.util.Collections;
import java.util.List;

public class JdbcSearchResponseResolver {
    private String oriSearchResponseGson;

    public JdbcSearchResponseResolver(String oriSearchResponseGson) {
        if (StringUtils.isBlank(oriSearchResponseGson)) {
            throw new IllegalArgumentException("param[oriSearchResponseGson] can not be blank");
        }
        this.oriSearchResponseGson = oriSearchResponseGson;
    }

    public String getOriSearchResponseGson() {
        return oriSearchResponseGson;
    }

    public <T> JdbcSearchResponse<T> resolveSearchResponse(Class<T> clazz) throws ResolveSearchResponseException {
        Gson defaultEsDateFormatGson = new GsonBuilder().setDateFormat(Constants.DEFAULT_ES_DATE_FORMAT).create();
        return resolveSearchResponse(clazz, defaultEsDateFormatGson);
    }

    public <T> JdbcSearchResponse<T> resolveSearchResponse(TypeToken<T> typeToken) throws ResolveSearchResponseException {
        Gson defaultEsDateFormatGson = new GsonBuilder().setDateFormat(Constants.DEFAULT_ES_DATE_FORMAT).create();
        return resolveSearchResponse(typeToken, defaultEsDateFormatGson);
    }

    public <T> JdbcSearchResponse<T> resolveSearchResponse(Class<T> clazz, String datePattern) throws ResolveSearchResponseException {
        Gson dateFormatGson = new GsonBuilder().setDateFormat(datePattern).create();
        return resolveSearchResponse(clazz, dateFormatGson);
    }

    public <T> JdbcSearchResponse<T> resolveSearchResponse(TypeToken<T> typeToken, String datePattern) throws ResolveSearchResponseException {
        Gson dateFormatGson = new GsonBuilder().setDateFormat(datePattern).create();
        return resolveSearchResponse(typeToken, dateFormatGson);
    }

    public <T> JdbcSearchResponse<T> resolveSearchResponse(Class<T> clazz, Gson gson) throws ResolveSearchResponseException {
        return resolveSearchResponse(new ResolveStrategy<T>() {
            @Override
            public T resolve(String resultItem) {
                return gson.fromJson(resultItem, clazz);
            }
        });
    }

    public <T> JdbcSearchResponse<T> resolveSearchResponse(TypeToken<T> typeToken, Gson gson) throws ResolveSearchResponseException {
        return resolveSearchResponse(new ResolveStrategy<T>() {
            @Override
            public T resolve(String resultItem) {
                return gson.fromJson(resultItem, typeToken.getType());
            }
        });
    }

    public <T> JdbcSearchResponse<T> resolveSearchResponse(ResolveStrategy<T> resolveStrategy) throws ResolveSearchResponseException {
        JdbcSearchResponse<String> searchRespStrGson = parseSearchResponseGson(oriSearchResponseGson);

        JdbcSearchResponse<T> retJdbcSearchResponse = new JdbcSearchResponse<T>();
        setBasicPropertyForResponse(retJdbcSearchResponse, searchRespStrGson);

        List<T> resultList = resolveResultList(searchRespStrGson.getResultList(), resolveStrategy);
        retJdbcSearchResponse.setResultList(resultList);

        return retJdbcSearchResponse;
    }

    protected <T> List<T> resolveResultList(List<String> resultList, ResolveStrategy<T> resolveStrategy) {
        if (CollectionUtils.isEmpty(resultList)) {
            return Collections.emptyList();
        }

        List<T> resolvedList = null;
        try {
            resolvedList = Lists.transform(resultList, new Function<String, T>() {
                @Override
                public T apply(String resultItem) {
                    return resolveStrategy.resolve(resultItem);
                }
            });
        }
        catch (Exception ex) {
            throw new ResolveSearchResponseException("Failed to resolve gson from response", ex);
        }
        return resolvedList;
    }

    protected JdbcSearchResponse<String> parseSearchResponseGson(String searchRespGson) {
        JdbcSearchResponse<String> searchRespStrGson;
        try {
            searchRespStrGson = new Gson().fromJson(searchRespGson, new TypeToken<JdbcSearchResponse<String>>() {
            }.getType());
        }
        catch (Exception exp) {
            throw new ResolveSearchResponseException(String.format("Failed to parse responseGson[%s] to JdbcSearchResponse", oriSearchResponseGson), exp);
        }
        return searchRespStrGson;
    }

    protected <T> void setBasicPropertyForResponse(JdbcSearchResponse<T> targetResponse, JdbcSearchResponse<String> sourceResponse) {
        targetResponse.setTotalShards(sourceResponse.getTotalShards());
        targetResponse.setTotalCount(sourceResponse.getTotalCount());
        targetResponse.setTookInMillis(sourceResponse.getTookInMillis());
        targetResponse.setFailedShards(sourceResponse.getFailedShards());
        targetResponse.setSuccessfulShards(sourceResponse.getSuccessfulShards());
    }

    public interface ResolveStrategy<T> {
        T resolve(String resultItem);
    }
}
