package org.elasticsearch.jdbc;

import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;

public class ElasticDriver implements Driver {

    private static final String ELASTIC_SEARCH_DRIVER_PREFIX = "jdbc:elastic:";

    private static final ElasticDriver driverInstance;

    static {
        driverInstance = new ElasticDriver();
        try {
            DriverManager.registerDriver(driverInstance);
        }
        catch (SQLException ex) {
            // ignore
        }
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        return new ElasticConnection();
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
