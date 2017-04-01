package org.elasticsearch;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.elasticsearch.es.JdbcSearchResponseResolver;
import org.springframework.dao.DataAccessException;

import java.util.List;

public interface ElasticSqlMapExecutor {

    // statementName

    <T> List<T> queryForList(String statementName, Class<T> clazz) throws DataAccessException;

    <T> List<T> queryForList(String statementName, Class<T> clazz, String datePattern) throws DataAccessException;

    <T> List<T> queryForList(String statementName, TypeToken<T> typeToken) throws DataAccessException;

    <T> List<T> queryForList(String statementName, TypeToken<T> typeToken, String datePattern) throws DataAccessException;

    <T> List<T> queryForList(String statementName, Class<T> clazz, Gson gson) throws DataAccessException;

    <T> List<T> queryForList(String statementName, TypeToken<T> typeToken, Gson gson) throws DataAccessException;

    <T> List<T> queryForList(String statementName, JdbcSearchResponseResolver.ResolveStrategy<T> resolveStrategy) throws DataAccessException;


    // String statementName, Object parameterObject


    <T> List<T> queryForList(String statementName, Object parameterObject, Class<T> clazz) throws DataAccessException;

    <T> List<T> queryForList(String statementName, Object parameterObject, Class<T> clazz, String datePattern) throws DataAccessException;

    <T> List<T> queryForList(String statementName, Object parameterObject, TypeToken<T> typeToken) throws DataAccessException;

    <T> List<T> queryForList(String statementName, Object parameterObject, TypeToken<T> typeToken, String datePattern) throws DataAccessException;

    <T> List<T> queryForList(String statementName, Object parameterObject, Class<T> clazz, Gson gson) throws DataAccessException;

    <T> List<T> queryForList(String statementName, Object parameterObject, TypeToken<T> typeToken, Gson gson) throws DataAccessException;

    <T> List<T> queryForList(String statementName, Object parameterObject, JdbcSearchResponseResolver.ResolveStrategy<T> resolveStrategy) throws DataAccessException;


    // statementName

    <T> T queryForObject(String statementName, Class<T> clazz) throws DataAccessException;

    <T> T queryForObject(String statementName, Class<T> clazz, String datePattern) throws DataAccessException;

    <T> T queryForObject(String statementName, TypeToken<T> typeToken) throws DataAccessException;

    <T> T queryForObject(String statementName, TypeToken<T> typeToken, String datePattern) throws DataAccessException;

    <T> T queryForObject(String statementName, Class<T> clazz, Gson gson) throws DataAccessException;

    <T> T queryForObject(String statementName, TypeToken<T> typeToken, Gson gson) throws DataAccessException;

    <T> T queryForObject(String statementName, JdbcSearchResponseResolver.ResolveStrategy<T> resolveStrategy) throws DataAccessException;


    // String statementName, Object parameterObject


    <T> T queryForObject(String statementName, Object parameterObject, Class<T> clazz) throws DataAccessException;

    <T> T queryForObject(String statementName, Object parameterObject, Class<T> clazz, String datePattern) throws DataAccessException;

    <T> T queryForObject(String statementName, Object parameterObject, TypeToken<T> typeToken) throws DataAccessException;

    <T> T queryForObject(String statementName, Object parameterObject, TypeToken<T> typeToken, String datePattern) throws DataAccessException;

    <T> T queryForObject(String statementName, Object parameterObject, Class<T> clazz, Gson gson) throws DataAccessException;

    <T> T queryForObject(String statementName, Object parameterObject, TypeToken<T> typeToken, Gson gson) throws DataAccessException;

    <T> T queryForObject(String statementName, Object parameterObject, JdbcSearchResponseResolver.ResolveStrategy<T> resolveStrategy) throws DataAccessException;

}
