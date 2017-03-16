package org.elasticsearch.jdbc;


import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;

public class SingleConnectionDataSource extends DriverManagerDataSource implements SmartDataSource {

    /**
     * Create a close-suppressing proxy?
     */
    private boolean suppressClose;

    /**
     * Override auto-commit state?
     */
    private Boolean autoCommit;

    /**
     * Wrapped Connection
     */
    private Connection target;

    /**
     * Proxy Connection
     */
    private Connection connection;

    /**
     * Synchronization monitor for the shared Connection
     */
    private final Object connectionMonitor = new Object();


    /**
     * Constructor for bean-style configuration.
     */
    public SingleConnectionDataSource() {
    }

    /**
     * Create a new SingleConnectionDataSource with the given standard
     * DriverManager parameters.
     *
     * @param driverClassName the JDBC driver class name
     * @param url             the JDBC URL to use for accessing the DriverManager
     * @param username        the JDBC username to use for accessing the DriverManager
     * @param password        the JDBC password to use for accessing the DriverManager
     * @param suppressClose   if the returned Connection should be a
     *                        close-suppressing proxy or the physical Connection
     * @see java.sql.DriverManager#getConnection(String, String, String)
     * @deprecated since Spring 2.5. Driver parameter usage is generally not recommended
     * for a SingleConnectionDataSource. If you insist on using driver parameters
     * directly, set up the Driver class manually before invoking this DataSource.
     */
    @Deprecated
    public SingleConnectionDataSource(
            String driverClassName, String url, String username, String password, boolean suppressClose) {

        super(driverClassName, url, username, password);
        this.suppressClose = suppressClose;
    }

    /**
     * Create a new SingleConnectionDataSource with the given standard
     * DriverManager parameters.
     *
     * @param url           the JDBC URL to use for accessing the DriverManager
     * @param username      the JDBC username to use for accessing the DriverManager
     * @param password      the JDBC password to use for accessing the DriverManager
     * @param suppressClose if the returned Connection should be a
     *                      close-suppressing proxy or the physical Connection
     * @see java.sql.DriverManager#getConnection(String, String, String)
     */
    public SingleConnectionDataSource(String url, String username, String password, boolean suppressClose) {
        super(url, username, password);
        this.suppressClose = suppressClose;
    }

    /**
     * Create a new SingleConnectionDataSource with the given standard
     * DriverManager parameters.
     *
     * @param url           the JDBC URL to use for accessing the DriverManager
     * @param suppressClose if the returned Connection should be a
     *                      close-suppressing proxy or the physical Connection
     * @see java.sql.DriverManager#getConnection(String, String, String)
     */
    public SingleConnectionDataSource(String url, boolean suppressClose) {
        super(url);
        this.suppressClose = suppressClose;
    }

    /**
     * Create a new SingleConnectionDataSource with a given Connection.
     *
     * @param target        underlying target Connection
     * @param suppressClose if the Connection should be wrapped with a Connection that
     *                      suppresses {@code close()} calls (to allow for normal {@code close()}
     *                      usage in applications that expect a pooled Connection but do not know our
     *                      SmartDataSource interface)
     */
    public SingleConnectionDataSource(Connection target, boolean suppressClose) {
        this.target = target;
        this.suppressClose = suppressClose;
        this.connection = (suppressClose ? getCloseSuppressingConnectionProxy(target) : target);
    }


    /**
     * Set whether the returned Connection should be a close-suppressing proxy
     * or the physical Connection.
     */
    public void setSuppressClose(boolean suppressClose) {
        this.suppressClose = suppressClose;
    }

    /**
     * Return whether the returned Connection will be a close-suppressing proxy
     * or the physical Connection.
     */
    protected boolean isSuppressClose() {
        return this.suppressClose;
    }

    /**
     * Set whether the returned Connection's "autoCommit" setting should be overridden.
     */
    public void setAutoCommit(boolean autoCommit) {
        this.autoCommit = (autoCommit);
    }

    /**
     * Return whether the returned Connection's "autoCommit" setting should be overridden.
     *
     * @return the "autoCommit" value, or {@code null} if none to be applied
     */
    protected Boolean getAutoCommitValue() {
        return this.autoCommit;
    }


    @Override
    public Connection getConnection() throws SQLException {
        synchronized (this.connectionMonitor) {
            if (this.connection == null) {
                // No underlying Connection -> lazy init via DriverManager.
                initConnection();
            }
            if (this.connection.isClosed()) {
                throw new SQLException(
                        "Connection was closed in SingleConnectionDataSource. Check that user code checks " +
                                "shouldClose() before closing Connections, or set 'suppressClose' to 'true'");
            }
            return this.connection;
        }
    }

    /**
     * Specifying a custom username and password doesn't make sense
     * with a single Connection. Returns the single Connection if given
     * the same username and password; throws a SQLException else.
     */
    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        return getConnection();
    }

    /**
     * This is a single Connection: Do not close it when returning to the "pool".
     */
    public boolean shouldClose(Connection con) {
        synchronized (this.connectionMonitor) {
            return (con != this.connection && con != this.target);
        }
    }

    /**
     * Close the underlying Connection.
     * The provider of this DataSource needs to care for proper shutdown.
     * <p>As this bean implements DisposableBean, a bean factory will
     * automatically invoke this on destruction of its cached singletons.
     */
    public void destroy() {
        synchronized (this.connectionMonitor) {
            closeConnection();
        }
    }


    /**
     * Initialize the underlying Connection via the DriverManager.
     */
    public void initConnection() throws SQLException {
        if (getUrl() == null) {
            throw new IllegalStateException("'url' property is required for lazily initializing a Connection");
        }
        synchronized (this.connectionMonitor) {
            closeConnection();
            this.target = getConnectionFromDriver(getUsername(), getPassword());
            prepareConnection(this.target);
            this.connection = (isSuppressClose() ? getCloseSuppressingConnectionProxy(this.target) : this.target);
        }
    }

    /**
     * Reset the underlying shared Connection, to be reinitialized on next access.
     */
    public void resetConnection() {
        synchronized (this.connectionMonitor) {
            closeConnection();
            this.target = null;
            this.connection = null;
        }
    }

    /**
     * Prepare the given Connection before it is exposed.
     * <p>The default implementation applies the auto-commit flag, if necessary.
     * Can be overridden in subclasses.
     *
     * @param con the Connection to prepare
     * @see #setAutoCommit
     */
    protected void prepareConnection(Connection con) throws SQLException {
        Boolean autoCommit = getAutoCommitValue();
        if (autoCommit != null && con.getAutoCommit() != autoCommit) {
            con.setAutoCommit(autoCommit);
        }
    }

    /**
     * Close the underlying shared Connection.
     */
    private void closeConnection() {
        if (this.target != null) {
            try {
                this.target.close();
            }
            catch (Throwable ex) {
                //logger.warn("Could not close shared JDBC Connection", ex);
            }
        }
    }

    /**
     * Wrap the given Connection with a proxy that delegates every method call to it
     * but suppresses close calls.
     *
     * @param target the original Connection to wrap
     * @return the wrapped Connection
     */
    protected Connection getCloseSuppressingConnectionProxy(Connection target) {
        return (Connection) Proxy.newProxyInstance(
                ConnectionProxy.class.getClassLoader(),
                new Class[]{ConnectionProxy.class},
                new CloseSuppressingInvocationHandler(target));
    }


    /**
     * Invocation handler that suppresses close calls on JDBC Connections.
     */
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

}
