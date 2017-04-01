package org.elasticsearch.api;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

public class ElasticResultSet extends AbstractResultSet {

    public static final ElasticResultSetMetaData resultSetMetaData = new ElasticResultSetMetaData();

    private String searchResultJson;

    private int rowCursor = 0;

    public ElasticResultSet(Statement statement, String searchResultJson) {
        super(statement);
        this.searchResultJson = searchResultJson;
    }

    @Override
    public boolean next() throws SQLException {
        rowCursor++;
        return rowCursor <= 1;
    }

    @Override
    public void beforeFirst() throws SQLException {
        rowCursor = 0;
    }

    @Override
    public void afterLast() throws SQLException {
        rowCursor = 1;
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        return searchResultJson;
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        return searchResultJson;
    }

    @Override
    public int getRow() throws SQLException {
        return rowCursor;
    }

    @Override
    public int getFetchSize() throws SQLException {
        return 1;
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return resultSetMetaData;
    }
}
