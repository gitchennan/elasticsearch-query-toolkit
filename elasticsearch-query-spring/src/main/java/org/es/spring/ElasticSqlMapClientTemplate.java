package org.es.spring;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.apache.commons.collections.CollectionUtils;
import org.es.jdbc.es.JdbcSearchResponse;
import org.es.jdbc.es.JdbcSearchResponseResolver;
import org.springframework.dao.DataAccessException;
import org.springframework.orm.ibatis.SqlMapClientTemplate;

import java.util.List;

public class ElasticSqlMapClientTemplate extends SqlMapClientTemplate implements ElasticSqlMapExecutor {
    @Override
    public <T> List<T> queryForList(String statementName, Class<T> clazz) throws DataAccessException {
        String responseGson = (String) super.queryForObject(statementName);
        JdbcSearchResponseResolver responseResolver = new JdbcSearchResponseResolver(responseGson);
        JdbcSearchResponse<T> searchResponse = responseResolver.resolveSearchResponse(clazz);
        return searchResponse.getResultList();
    }

    @Override
    public <T> List<T> queryForList(String statementName, Class<T> clazz, String datePattern) throws DataAccessException {
        String responseGson = (String) super.queryForObject(statementName);
        JdbcSearchResponseResolver responseResolver = new JdbcSearchResponseResolver(responseGson);
        JdbcSearchResponse<T> searchResponse = responseResolver.resolveSearchResponse(clazz, datePattern);
        return searchResponse.getResultList();
    }

    @Override
    public <T> List<T> queryForList(String statementName, TypeToken<T> typeToken) throws DataAccessException {
        String responseGson = (String) super.queryForObject(statementName);
        JdbcSearchResponseResolver responseResolver = new JdbcSearchResponseResolver(responseGson);
        JdbcSearchResponse<T> searchResponse = responseResolver.resolveSearchResponse(typeToken);
        return searchResponse.getResultList();
    }

    @Override
    public <T> List<T> queryForList(String statementName, TypeToken<T> typeToken, String datePattern) throws DataAccessException {
        String responseGson = (String) super.queryForObject(statementName);
        JdbcSearchResponseResolver responseResolver = new JdbcSearchResponseResolver(responseGson);
        JdbcSearchResponse<T> searchResponse = responseResolver.resolveSearchResponse(typeToken, datePattern);
        return searchResponse.getResultList();
    }

    @Override
    public <T> List<T> queryForList(String statementName, Class<T> clazz, Gson gson) throws DataAccessException {
        String responseGson = (String) super.queryForObject(statementName);
        JdbcSearchResponseResolver responseResolver = new JdbcSearchResponseResolver(responseGson);
        JdbcSearchResponse<T> searchResponse = responseResolver.resolveSearchResponse(clazz, gson);
        return searchResponse.getResultList();
    }

    @Override
    public <T> List<T> queryForList(String statementName, TypeToken<T> typeToken, Gson gson) throws DataAccessException {
        String responseGson = (String) super.queryForObject(statementName);
        JdbcSearchResponseResolver responseResolver = new JdbcSearchResponseResolver(responseGson);
        JdbcSearchResponse<T> searchResponse = responseResolver.resolveSearchResponse(typeToken, gson);
        return searchResponse.getResultList();
    }

    @Override
    public <T> List<T> queryForList(String statementName, JdbcSearchResponseResolver.ResolveStrategy<T> resolveStrategy) throws DataAccessException {
        String responseGson = (String) super.queryForObject(statementName);
        JdbcSearchResponseResolver responseResolver = new JdbcSearchResponseResolver(responseGson);
        JdbcSearchResponse<T> searchResponse = responseResolver.resolveSearchResponse(resolveStrategy);
        return searchResponse.getResultList();
    }

    @Override
    public <T> List<T> queryForList(String statementName, Object parameterObject, Class<T> clazz) throws DataAccessException {
        String responseGson = (String) super.queryForObject(statementName, parameterObject);
        JdbcSearchResponseResolver responseResolver = new JdbcSearchResponseResolver(responseGson);
        JdbcSearchResponse<T> searchResponse = responseResolver.resolveSearchResponse(clazz);
        return searchResponse.getResultList();
    }

    @Override
    public <T> List<T> queryForList(String statementName, Object parameterObject, Class<T> clazz, String datePattern) throws DataAccessException {
        String responseGson = (String) super.queryForObject(statementName, parameterObject);
        JdbcSearchResponseResolver responseResolver = new JdbcSearchResponseResolver(responseGson);
        JdbcSearchResponse<T> searchResponse = responseResolver.resolveSearchResponse(clazz, datePattern);
        return searchResponse.getResultList();
    }

    @Override
    public <T> List<T> queryForList(String statementName, Object parameterObject, TypeToken<T> typeToken) throws DataAccessException {
        String responseGson = (String) super.queryForObject(statementName, parameterObject);
        JdbcSearchResponseResolver responseResolver = new JdbcSearchResponseResolver(responseGson);
        JdbcSearchResponse<T> searchResponse = responseResolver.resolveSearchResponse(typeToken);
        return searchResponse.getResultList();
    }

    @Override
    public <T> List<T> queryForList(String statementName, Object parameterObject, TypeToken<T> typeToken, String datePattern) throws DataAccessException {
        String responseGson = (String) super.queryForObject(statementName, parameterObject);
        JdbcSearchResponseResolver responseResolver = new JdbcSearchResponseResolver(responseGson);
        JdbcSearchResponse<T> searchResponse = responseResolver.resolveSearchResponse(typeToken, datePattern);
        return searchResponse.getResultList();
    }

    @Override
    public <T> List<T> queryForList(String statementName, Object parameterObject, Class<T> clazz, Gson gson) throws DataAccessException {
        String responseGson = (String) super.queryForObject(statementName, parameterObject);
        JdbcSearchResponseResolver responseResolver = new JdbcSearchResponseResolver(responseGson);
        JdbcSearchResponse<T> searchResponse = responseResolver.resolveSearchResponse(clazz, gson);
        return searchResponse.getResultList();
    }

    @Override
    public <T> List<T> queryForList(String statementName, Object parameterObject, TypeToken<T> typeToken, Gson gson) throws DataAccessException {
        String responseGson = (String) super.queryForObject(statementName, parameterObject);
        JdbcSearchResponseResolver responseResolver = new JdbcSearchResponseResolver(responseGson);
        JdbcSearchResponse<T> searchResponse = responseResolver.resolveSearchResponse(typeToken, gson);
        return searchResponse.getResultList();
    }

    @Override
    public <T> List<T> queryForList(String statementName, Object parameterObject, JdbcSearchResponseResolver.ResolveStrategy<T> resolveStrategy) throws DataAccessException {
        String responseGson = (String) super.queryForObject(statementName, parameterObject);
        JdbcSearchResponseResolver responseResolver = new JdbcSearchResponseResolver(responseGson);
        JdbcSearchResponse<T> searchResponse = responseResolver.resolveSearchResponse(resolveStrategy);
        return searchResponse.getResultList();
    }

    @Override
    public <T> T queryForObject(String statementName, Class<T> clazz) throws DataAccessException {
        List<T> resultList = queryForList(statementName, clazz);
        return CollectionUtils.isEmpty(resultList) ? null : resultList.get(0);
    }

    @Override
    public <T> T queryForObject(String statementName, Class<T> clazz, String datePattern) throws DataAccessException {
        List<T> resultList = queryForList(statementName, clazz, datePattern);
        return CollectionUtils.isEmpty(resultList) ? null : resultList.get(0);
    }

    @Override
    public <T> T queryForObject(String statementName, TypeToken<T> typeToken) throws DataAccessException {
        List<T> resultList = queryForList(statementName, typeToken);
        return CollectionUtils.isEmpty(resultList) ? null : resultList.get(0);
    }

    @Override
    public <T> T queryForObject(String statementName, TypeToken<T> typeToken, String datePattern) throws DataAccessException {
        List<T> resultList = queryForList(statementName, typeToken, datePattern);
        return CollectionUtils.isEmpty(resultList) ? null : resultList.get(0);
    }

    @Override
    public <T> T queryForObject(String statementName, Class<T> clazz, Gson gson) throws DataAccessException {
        List<T> resultList = queryForList(statementName, clazz, gson);
        return CollectionUtils.isEmpty(resultList) ? null : resultList.get(0);
    }

    @Override
    public <T> T queryForObject(String statementName, TypeToken<T> typeToken, Gson gson) throws DataAccessException {
        List<T> resultList = queryForList(statementName, typeToken, gson);
        return CollectionUtils.isEmpty(resultList) ? null : resultList.get(0);
    }

    @Override
    public <T> T queryForObject(String statementName, JdbcSearchResponseResolver.ResolveStrategy<T> resolveStrategy) throws DataAccessException {
        List<T> resultList = queryForList(statementName, resolveStrategy);
        return CollectionUtils.isEmpty(resultList) ? null : resultList.get(0);
    }

    @Override
    public <T> T queryForObject(String statementName, Object parameterObject, Class<T> clazz) throws DataAccessException {
        List<T> resultList = queryForList(statementName, parameterObject, clazz);
        return CollectionUtils.isEmpty(resultList) ? null : resultList.get(0);
    }

    @Override
    public <T> T queryForObject(String statementName, Object parameterObject, Class<T> clazz, String datePattern) throws DataAccessException {
        List<T> resultList = queryForList(statementName, parameterObject, clazz, datePattern);
        return CollectionUtils.isEmpty(resultList) ? null : resultList.get(0);
    }

    @Override
    public <T> T queryForObject(String statementName, Object parameterObject, TypeToken<T> typeToken) throws DataAccessException {
        List<T> resultList = queryForList(statementName, parameterObject, typeToken);
        return CollectionUtils.isEmpty(resultList) ? null : resultList.get(0);
    }

    @Override
    public <T> T queryForObject(String statementName, Object parameterObject, TypeToken<T> typeToken, String datePattern) throws DataAccessException {
        List<T> resultList = queryForList(statementName, parameterObject, typeToken, datePattern);
        return CollectionUtils.isEmpty(resultList) ? null : resultList.get(0);
    }

    @Override
    public <T> T queryForObject(String statementName, Object parameterObject, Class<T> clazz, Gson gson) throws DataAccessException {
        List<T> resultList = queryForList(statementName, parameterObject, clazz, gson);
        return CollectionUtils.isEmpty(resultList) ? null : resultList.get(0);
    }

    @Override
    public <T> T queryForObject(String statementName, Object parameterObject, TypeToken<T> typeToken, Gson gson) throws DataAccessException {
        List<T> resultList = queryForList(statementName, parameterObject, typeToken, gson);
        return CollectionUtils.isEmpty(resultList) ? null : resultList.get(0);
    }

    @Override
    public <T> T queryForObject(String statementName, Object parameterObject, JdbcSearchResponseResolver.ResolveStrategy<T> resolveStrategy) throws DataAccessException {
        List<T> resultList = queryForList(statementName, parameterObject, resolveStrategy);
        return CollectionUtils.isEmpty(resultList) ? null : resultList.get(0);
    }
}
