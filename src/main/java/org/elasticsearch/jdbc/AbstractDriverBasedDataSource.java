package org.elasticsearch.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

public abstract class AbstractDriverBasedDataSource extends AbstractDataSource {

    private String url;

    private String username;

    private String password;

    private Properties connectionProperties;

    public void setUrl(String url) {
        this.url = url.trim();
    }

    public String getUrl() {
        return this.url;
    }


    public void setUsername(String username) {
        this.username = username;
    }


    public String getUsername() {
        return this.username;
    }


    public void setPassword(String password) {
        this.password = password;
    }

    public String getPassword() {
        return this.password;
    }


    public void setConnectionProperties(Properties connectionProperties) {
        this.connectionProperties = connectionProperties;
    }

    public Properties getConnectionProperties() {
        return this.connectionProperties;
    }

    public Connection getConnection() throws SQLException {
        return getConnectionFromDriver(getUsername(), getPassword());
    }

    public Connection getConnection(String username, String password) throws SQLException {
        return getConnectionFromDriver(username, password);
    }

    protected Connection getConnectionFromDriver(String username, String password) throws SQLException {
        Properties mergedProps = new Properties();
        Properties connProps = getConnectionProperties();
        if (connProps != null) {
            mergedProps.putAll(connProps);
        }
        if (username != null) {
            mergedProps.setProperty("user", username);
        }
        if (password != null) {
            mergedProps.setProperty("password", password);
        }
        return getConnectionFromDriver(mergedProps);
    }

    protected abstract Connection getConnectionFromDriver(Properties props) throws SQLException;
}
