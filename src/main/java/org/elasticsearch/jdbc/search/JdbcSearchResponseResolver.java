package org.elasticsearch.jdbc.search;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.jdbc.exception.ResolveSearchResponseException;
import org.elasticsearch.utils.Constants;

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

    public <T> JdbcSearchResponse<T> resolveSearchResponse(Class<T> clazz, String dataPattern) throws ResolveSearchResponseException {
        Gson dateFormatGson = new GsonBuilder().setDateFormat(dataPattern).create();
        return resolveSearchResponse(clazz, dateFormatGson);
    }

    public <T> JdbcSearchResponse<T> resolveSearchResponse(TypeToken<T> typeToken, String dataPattern) throws ResolveSearchResponseException {
        Gson dateFormatGson = new GsonBuilder().setDateFormat(dataPattern).create();
        return resolveSearchResponse(typeToken, dateFormatGson);
    }

    public <T> JdbcSearchResponse<T> resolveSearchResponse(Class<T> clazz, Gson gson) throws ResolveSearchResponseException {
        return resolveSearchResponse(new ResolveStrategy<T>() {
            @Override
            public T resolve(String document) {
                return gson.fromJson(document, clazz);
            }
        });
    }

    public <T> JdbcSearchResponse<T> resolveSearchResponse(TypeToken<T> typeToken, Gson gson) throws ResolveSearchResponseException {
        return resolveSearchResponse(new ResolveStrategy<T>() {
            @Override
            public T resolve(String document) {
                return gson.fromJson(document, typeToken.getType());
            }
        });
    }

    public <T> JdbcSearchResponse<T> resolveSearchResponse(ResolveStrategy<T> resolveStrategy) throws ResolveSearchResponseException {
        JdbcSearchResponse<String> searchRespStrGson = parseSearchResponseGson(oriSearchResponseGson);

        JdbcSearchResponse<T> retJdbcSearchResponse = new JdbcSearchResponse<T>();
        setBasicPropertyForResponse(retJdbcSearchResponse, searchRespStrGson);

        // resolve query result set
        List<T> docList = resolveDocumentList(searchRespStrGson.getDocList(), resolveStrategy);
        retJdbcSearchResponse.setDocList(docList);

        // todo resolve aggregation result set

        return retJdbcSearchResponse;
    }

    protected <T> List<T> resolveDocumentList(List<String> documentList, ResolveStrategy<T> resolveStrategy) {
        if (CollectionUtils.isEmpty(documentList)) {
            return Collections.emptyList();
        }

        List<T> resolvedDocList = null;
        try {
            resolvedDocList = Lists.transform(documentList, new Function<String, T>() {
                @Override
                public T apply(String doc) {
                    return resolveStrategy.resolve(doc);
                }
            });
        }
        catch (Exception ex) {
            throw new ResolveSearchResponseException("Failed to resolve doc gson");
        }
        return resolvedDocList;
    }

    protected JdbcSearchResponse<String> parseSearchResponseGson(String searchRespGson) {
        JdbcSearchResponse<String> searchRespStrGson;
        try {
            searchRespStrGson = new Gson().fromJson(oriSearchResponseGson, new TypeToken<JdbcSearchResponse<String>>() {}.getType());
        }
        catch (Exception exp) {
            throw new ResolveSearchResponseException(String.format("Failed to parse responseGson[%s] to JdbcSearchResponse", oriSearchResponseGson));
        }
        return searchRespStrGson;
    }

    protected <T> void setBasicPropertyForResponse(JdbcSearchResponse<T> targetResponse, JdbcSearchResponse<String> sourceResponse) {
        targetResponse.setTotalShards(sourceResponse.getTotalShards());
        targetResponse.setTotalHits(sourceResponse.getTotalHits());
        targetResponse.setTookInMillis(sourceResponse.getTookInMillis());
        targetResponse.setFailedShards(sourceResponse.getFailedShards());
        targetResponse.setSuccessfulShards(sourceResponse.getSuccessfulShards());
    }

    public interface ResolveStrategy<T> {
        T resolve(String document);
    }
}
