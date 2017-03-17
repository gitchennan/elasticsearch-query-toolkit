package org.elasticsearch.jdbc;

import org.elasticsearch.client.Client;

import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;

public class ElasticDriver implements Driver {

    private static final String ELASTIC_SEARCH_DRIVER_PREFIX = "jdbc:elastic:";

    private Client client;

    static {
        try {
            DriverManager.registerDriver(new ElasticDriver());
        }
        catch (SQLException ex) {
            // ignore
        }
    }

    private ElasticDriver() {

    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        String ipUrl = url.substring(ELASTIC_SEARCH_DRIVER_PREFIX.length() - 1);
        Client client = TransportClientFactory.createTransportClientFromUrl(url);
        return new ElasticConnection(url, info, client);
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return url != null && url.startsWith(ELASTIC_SEARCH_DRIVER_PREFIX);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
        return 1;
    }

    @Override
    public int getMinorVersion() {
        return 0;
    }

    @Override
    public boolean jdbcCompliant() {
        return true;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new UnsupportedOperationException("getParentLogger");
    }
}
