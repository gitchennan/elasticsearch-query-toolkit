package org.elasticsearch.jdbc;


import org.elasticsearch.client.Client;
import org.elasticsearch.jdbc.search.TransportClientProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;

public class ElasticSingleConnectionDataSource extends DriverManagerDataSource implements SmartDataSource {

    private static final Logger logger = LoggerFactory.getLogger(ElasticSingleConnectionDataSource.class);

    private boolean suppressClose;

    private Connection target;

    private Connection connection;

    private Client client;

    private TransportClientProvider transportClientProvider;

    private final Object connectionMonitor = new Object();

    public ElasticSingleConnectionDataSource() {

    }

    public ElasticSingleConnectionDataSource(String url, boolean suppressClose) {
        super(url);
        this.suppressClose = suppressClose;
    }

    public void setTransportClientProvider(TransportClientProvider transportClientProvider) {
        this.transportClientProvider = transportClientProvider;
    }

    public void setSuppressClose(boolean suppressClose) {
        this.suppressClose = suppressClose;
    }

    protected boolean isSuppressClose() {
        return this.suppressClose;
    }

    @Override
    public Connection getConnection() throws SQLException {
        synchronized (this.connectionMonitor) {
            if (this.connection == null) {
                initConnection();
            }
            if (this.connection.isClosed()) {
                throw new SQLException(
                        "Connection was closed in ElasticSingleConnectionDataSource. Check that user code checks " +
                                "shouldClose() before closing Connections, or set 'suppressClose' to 'true'");
            }
            return this.connection;
        }
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        return getConnection();
    }


    public boolean shouldClose(Connection con) {
        synchronized (this.connectionMonitor) {
            return (con != this.connection && con != this.target);
        }
    }

    public void destroy() {
        synchronized (this.connectionMonitor) {
            closeConnection();
        }
    }


    public void initConnection() throws SQLException {
        if (getUrl() == null) {
            throw new IllegalStateException("'url' property is required for lazily initializing a Connection");
        }
        synchronized (this.connectionMonitor) {
            closeConnection();

            try {
                if (transportClientProvider != null) {
                    client = transportClientProvider.createTransportClientFromUrl(getUrl());
                    if (client == null) {
                        throw new SQLException(String.format("Failed to build transport client for url[%s]", getUrl()));
                    }
                    target = new ElasticConnection(getUrl(), null, client);
                }
                else {
                    this.target = getConnectionFromDriver(getUsername(), getPassword());
                }
            }
            catch (Exception exp) {
                throw new SQLException(String.format("Failed to create connection for url[%s]", getUrl()), exp);
            }

            prepareConnection(target);
            this.connection = (isSuppressClose() ? getCloseSuppressingConnectionProxy(target) : target);
        }
    }


    public void resetConnection() {
        synchronized (this.connectionMonitor) {
            closeConnection();
            this.target = null;
            this.connection = null;
        }
    }


    protected void prepareConnection(Connection con) throws SQLException {
        Boolean autoCommit = getAutoCommitValue();
        if (autoCommit != null && con.getAutoCommit() != autoCommit) {
            con.setAutoCommit(autoCommit);
        }
    }


    private void closeConnection() {
        if (this.target != null) {
            try {
                this.target.close();
            }
            catch (Throwable ex) {
                logger.warn("Could not close shared JDBC Connection", ex);
            }
        }

        if (client != null) {
            try {
                client.close();
            }
            catch (Exception ex) {
                logger.error("Could not close elasticsearch client", ex);
            }
        }
    }


    protected Connection getCloseSuppressingConnectionProxy(Connection target) {
        return (Connection) Proxy.newProxyInstance(
                ConnectionProxy.class.getClassLoader(),
                new Class[]{ConnectionProxy.class},
                new CloseSuppressingInvocationHandler(target));
    }


    private static class CloseSuppressingInvocationHandler implements InvocationHandler {
        private final Connection target;

        public CloseSuppressingInvocationHandler(Connection target) {
            this.target = target;
        }

        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            // Invocation on ConnectionProxy interface coming in...

            if (method.getName().equals("equals")) {
                // Only consider equal when proxies are identical.
                return (proxy == args[0]);
            }
            else if (method.getName().equals("hashCode")) {
                // Use hashCode of Connection proxy.
                return System.identityHashCode(proxy);
            }
            else if (method.getName().equals("unwrap")) {
                if (((Class) args[0]).isInstance(proxy)) {
                    return proxy;
                }
            }
            else if (method.getName().equals("isWrapperFor")) {
                if (((Class) args[0]).isInstance(proxy)) {
                    return true;
                }
            }
            else if (method.getName().equals("close")) {
                // Handle close method: don't pass the call on.
                return null;
            }
            else if (method.getName().equals("isClosed")) {
                return false;
            }
            else if (method.getName().equals("getTargetConnection")) {
                // Handle getTargetConnection method: return underlying Connection.
                return this.target;
            }

            // Invoke method on target Connection.
            try {
                return method.invoke(this.target, args);
            }
            catch (InvocationTargetException ex) {
                throw ex.getTargetException();
            }
        }
    }

    protected Boolean getAutoCommitValue() {
        return Boolean.FALSE;
    }

    public void setAutoCommit(boolean autoCommit) {
        // ignore
    }

}
