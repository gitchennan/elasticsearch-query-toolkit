package org.elasticsearch.jdbc.api;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

public abstract class AbstractDriverBasedDataSource extends AbstractDataSource {

    private String url;

    private Properties connectionProperties;

    public void setUrl(String url) {
        this.url = url.trim();
    }

    public String getUrl() {
        return this.url;
    }

    public void setConnectionProperties(Properties connectionProperties) {
        this.connectionProperties = connectionProperties;
    }

    public Properties getConnectionProperties() {
        return this.connectionProperties;
    }

    public Connection getConnection() throws SQLException {
        return getConnectionFromDriver();
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        return getConnection();
    }

    protected Connection getConnectionFromDriver() throws SQLException {
        Properties mergedProps = new Properties();
        Properties connProps = getConnectionProperties();
        if (connProps != null) {
            mergedProps.putAll(connProps);
        }
        return getConnectionFromDriver(mergedProps);
    }

    protected abstract Connection getConnectionFromDriver(Properties props) throws SQLException;
}
