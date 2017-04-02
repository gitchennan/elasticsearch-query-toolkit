package org.es.jdbc.api;

import org.es.jdbc.es.ElasticClientProvider;
import org.es.jdbc.es.ElasticClientProxy;
import org.es.jdbc.es.ElasticClientProxyProviderImpl;

import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;

public class ElasticDriver implements Driver {

    private static final String ELASTIC_SEARCH_DRIVER_PREFIX = "jdbc:elastic:";

    private ElasticClientProxy elasticClientProxy = null;

    private ElasticClientProvider elasticClientProvider;

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
            if (elasticClientProxy != null && Boolean.FALSE == elasticClientProxy.isClosed()) {
                return new ElasticConnection(url, info, elasticClientProxy);
            }

            if (elasticClientProvider == null) {
                elasticClientProvider = new ElasticClientProxyProviderImpl();
            }

            elasticClientProxy = (ElasticClientProxy) elasticClientProvider.createElasticClientFromUrl(url);
            if (elasticClientProxy == null || elasticClientProxy.isClosed()) {
                throw new SQLException(String.format("ElasticDriver.connect] Failed to build elastic client for url[%s]", url));
            }
        }
        return new ElasticConnection(url, info, elasticClientProxy);
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
