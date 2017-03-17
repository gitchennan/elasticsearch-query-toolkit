package org.elasticsearch.jdbc;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.dsl.bean.ElasticSqlParseResult;
import org.elasticsearch.dsl.parser.ElasticSql2DslParser;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public class ElasticStatement extends AbstractStatement {

    protected ElasticConnection connection;

    public ElasticStatement(ElasticConnection connection) {
        this.connection = connection;
    }

    private ResultSet executeResult;

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        ElasticSql2DslParser sql2DslParser = new ElasticSql2DslParser();
        ElasticSqlParseResult parseResult = sql2DslParser.parse(sql);

        SearchRequestBuilder searchRequest = parseResult.toRequest(connection.getClient());
        SearchResponse searchResponse = searchRequest.execute().actionGet();

        return executeResult = new ElasticResultSet(this, searchResponse.toString());
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