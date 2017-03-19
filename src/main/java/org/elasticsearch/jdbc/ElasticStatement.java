package org.elasticsearch.jdbc;

import com.google.common.collect.Lists;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.dsl.bean.ElasticSqlParseResult;
import org.elasticsearch.dsl.parser.ElasticSql2DslParser;
import org.elasticsearch.jdbc.search.JdbcSearchActionExecutor;
import org.elasticsearch.jdbc.search.JdbcSearchResponse;
import org.elasticsearch.search.SearchHit;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class ElasticStatement extends AbstractStatement {
    protected ElasticConnection connection;

    public ElasticStatement(ElasticConnection connection) {
        this.connection = connection;
    }

    protected ResultSet executeResult;

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
            throw new SQLException(String.format("[ElasticStatement] Failed to execute search request sql[%s]", sql), exp);
        }

        JdbcSearchResponse<String> jdbcSearchResponse = new JdbcSearchResponse<String>();
        jdbcSearchResponse.setFailedShards(searchResponse.getFailedShards());
        jdbcSearchResponse.setSuccessfulShards(searchResponse.getSuccessfulShards());
        jdbcSearchResponse.setTookInMillis(searchResponse.getTookInMillis());
        jdbcSearchResponse.setTotalShards(searchResponse.getTotalShards());
        jdbcSearchResponse.setTotalHits(searchResponse.getHits().getTotalHits());

        List<String> hits = Lists.newLinkedList();
        for (SearchHit searchHit : searchResponse.getHits().getHits()) {
            hits.add(searchHit.getSourceAsString());
        }
        jdbcSearchResponse.setDocList(hits);
        return executeResult = new ElasticResultSet(this, jdbcSearchResponse.toJson());
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        executeQuery(sql);
        return true;
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        return executeResult;
    }

    @Override
    public Connection getConnection() throws SQLException {
        return connection;
    }
}