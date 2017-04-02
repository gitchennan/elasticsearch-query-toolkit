package org.es.jdbc.api;

import org.elasticsearch.client.Client;

import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class ElasticConnection extends AbstractConnection {

    private Client client;

    public ElasticConnection(String url, Properties info, Client client) {
        super(url, info);
        this.client = client;
    }

    Client getClient() {
        return client;
    }

    @Override
    public Statement createStatement() throws SQLException {
        return new ElasticStatement(this);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return new ElasticPreparedStatement(this, sql);
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        return new ElasticDatabaseMetaData(url);
    }

}
