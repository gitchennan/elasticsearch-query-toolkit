package org.elasticsearch.jdbc;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.jdbc.search.TransportClientProvider;
import org.elasticsearch.jdbc.search.TransportClientProviderImpl;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;

public class ElasticDriver implements Driver {

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(ElasticDriver.class);

    private static final String ELASTIC_SEARCH_DRIVER_PREFIX = "jdbc:elastic:";

    private TransportClient transportClient = null;

    private TransportClientProvider transportClientProvider;

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
        synchronized (ElasticDriver.class) {
            if (transportClientProvider == null) {
                transportClientProvider = new TransportClientProviderImpl();
            }
            transportClient = transportClientProvider.createTransportClientFromUrl(url);
            if (transportClient == null) {
                throw new SQLException(String.format("ElasticDriver.connect] Failed to build transport client for url[%s]", url));
            }
        }
        return new ElasticConnection(url, info, transportClient);
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
