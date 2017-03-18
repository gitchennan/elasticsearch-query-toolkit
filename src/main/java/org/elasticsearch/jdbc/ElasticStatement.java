package org.elasticsearch.jdbc;

import com.google.common.collect.Lists;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.dsl.bean.ElasticSqlParseResult;
import org.elasticsearch.dsl.parser.ElasticSql2DslParser;
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
        ElasticSql2DslParser sql2DslParser = new ElasticSql2DslParser();
        ElasticSqlParseResult parseResult = sql2DslParser.parse(sql);

        SearchRequestBuilder searchRequest = parseResult.toRequest(connection.getClient());
        SearchResponse searchResponse = SearchActionExecutor.get().syncExecute(searchRequest);

        SearchResponseGson searchResponseGson = new SearchResponseGson();
        searchResponseGson.setFailedShards(searchResponse.getFailedShards());
        searchResponseGson.setSuccessfulShards(searchResponse.getSuccessfulShards());
        searchResponseGson.setTookInMillis(searchResponse.getTookInMillis());
        searchResponseGson.setTotalShards(searchResponse.getTotalShards());
        searchResponseGson.setTotalHits(searchResponse.getHits().getTotalHits());

        List<String> hits = Lists.newLinkedList();
        for (SearchHit searchHit : searchResponse.getHits().getHits()) {
            hits.add(searchHit.getSourceAsString());
        }
        searchResponseGson.setDocList(hits);

        return executeResult = new ElasticResultSet(this, searchResponseGson.toJson());
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