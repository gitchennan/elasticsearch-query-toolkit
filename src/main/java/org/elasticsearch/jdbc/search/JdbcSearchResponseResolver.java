package org.elasticsearch.jdbc.search;

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
            throw new IllegalArgumentException("param[oriSearchResponseGson] can not be null");
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

    public <T> JdbcSearchResponse<T> resolveSearchResponse(Class<T> clazz, String dataPattern) throws ResolveSearchResponseException {
        Gson defaultEsDateFormatGson = new GsonBuilder().setDateFormat(dataPattern).create();
        return resolveSearchResponse(clazz, defaultEsDateFormatGson);
    }

    public <T> JdbcSearchResponse<T> resolveSearchResponse(Class<T> clazz, Gson gson) throws ResolveSearchResponseException {
        return resolveSearchResponse(new ResolveStrategy<T>() {
            @Override
            public T resolve(String document) {
                return gson.fromJson(document, clazz);
            }
        });
    }

    public <T> JdbcSearchResponse<T> resolveSearchResponse(ResolveStrategy<T> resolveStrategy) throws ResolveSearchResponseException {
        JdbcSearchResponse<String> searchRespStrGson;
        try {
            searchRespStrGson = new Gson().fromJson(oriSearchResponseGson, new TypeToken<JdbcSearchResponse<String>>() {}.getType());
        }
        catch (Exception exp) {
            throw new ResolveSearchResponseException(String.format("Failed to parse responseGson[%s] to JdbcSearchResponse", oriSearchResponseGson));
        }

        JdbcSearchResponse<T> jdbcSearchResponse = new JdbcSearchResponse<T>();
        jdbcSearchResponse.setTotalShards(searchRespStrGson.getTotalShards());
        jdbcSearchResponse.setTotalHits(searchRespStrGson.getTotalHits());
        jdbcSearchResponse.setTookInMillis(searchRespStrGson.getTookInMillis());
        jdbcSearchResponse.setFailedShards(searchRespStrGson.getFailedShards());
        jdbcSearchResponse.setSuccessfulShards(searchRespStrGson.getSuccessfulShards());

        if (CollectionUtils.isEmpty(searchRespStrGson.getDocList())) {
            jdbcSearchResponse.setDocList(Collections.emptyList());
            return jdbcSearchResponse;
        }

        List<T> docList = Lists.newLinkedList();
        for (String doc : searchRespStrGson.getDocList()) {
            docList.add(resolveStrategy.resolve(doc));
        }
        jdbcSearchResponse.setDocList(docList);
        return jdbcSearchResponse;
    }

    public interface ResolveStrategy<T> {
        T resolve(String document);
    }
}
