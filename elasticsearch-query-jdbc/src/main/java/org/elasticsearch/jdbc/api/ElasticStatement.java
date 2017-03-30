package org.elasticsearch.jdbc.api;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.dsl.bean.ElasticSqlParseResult;
import org.elasticsearch.dsl.parser.ElasticSql2DslParser;
import org.elasticsearch.jdbc.es.JdbcSearchActionExecutor;
import org.elasticsearch.jdbc.es.JdbcSearchResponseExtractor;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public class ElasticStatement extends AbstractStatement {
    protected ElasticConnection connection;
    protected ResultSet executeResult;

    public ElasticStatement(ElasticConnection connection) {
        this.connection = connection;
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        return executeResult;
    }

    @Override
    public Connection getConnection() throws SQLException {
        return connection;
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        executeQuery(sql);
        return true;
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        return executeQuery(sql, null);
    }

    @Override
    public ResultSet executeQuery(String sql, Object[] args) throws SQLException {
        ElasticSqlParseResult parseResult;
        try {
            ElasticSql2DslParser sql2DslParser = new ElasticSql2DslParser();
            parseResult = sql2DslParser.parse(sql, args);
        }
        catch (Exception exp) {
            throw new SQLException(String.format("[ElasticStatement] Failed to parse sql[%s]", sql), exp);
        }

        SearchResponse searchResponse;
        try {
            SearchRequestBuilder searchRequest = parseResult.toRequest(connection.getClient());
            searchResponse = JdbcSearchActionExecutor.get().syncExecuteWithException(searchRequest);
        }
        catch (Exception exp) {
            throw new SQLException(String.format("[ElasticStatement] Failed to execute es request sql[%s]", sql), exp);
        }

        JdbcSearchResponseExtractor responseExtractor = new JdbcSearchResponseExtractor();
        String searchResponseGson = responseExtractor.extractSearchResponse(searchResponse);
        return executeResult = new ElasticResultSet(this, searchResponseGson);
    }
}