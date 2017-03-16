package org.elasticsearch.jdbc;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class DriverManagerDataSource extends AbstractDriverBasedDataSource {

    /**
     * Constructor for bean-style configuration.
     */
    public DriverManagerDataSource() {
    }

    /**
     * Create a new DriverManagerDataSource with the given JDBC URL,
     * not specifying a username or password for JDBC access.
     *
     * @param url the JDBC URL to use for accessing the DriverManager
     * @see java.sql.DriverManager#getConnection(String)
     */
    public DriverManagerDataSource(String url) {
        setUrl(url);
    }

    /**
     * Create a new DriverManagerDataSource with the given standard
     * DriverManager parameters.
     *
     * @param url      the JDBC URL to use for accessing the DriverManager
     * @param username the JDBC username to use for accessing the DriverManager
     * @param password the JDBC password to use for accessing the DriverManager
     * @see java.sql.DriverManager#getConnection(String, String, String)
     */
    public DriverManagerDataSource(String url, String username, String password) {
        setUrl(url);
        setUsername(username);
        setPassword(password);
    }

    /**
     * Create a new DriverManagerDataSource with the given JDBC URL,
     * not specifying a username or password for JDBC access.
     *
     * @param url      the JDBC URL to use for accessing the DriverManager
     * @param conProps JDBC connection properties
     * @see java.sql.DriverManager#getConnection(String)
     */
    public DriverManagerDataSource(String url, Properties conProps) {
        setUrl(url);
        setConnectionProperties(conProps);
    }

    @Deprecated
    public DriverManagerDataSource(String driverClassName, String url, String username, String password) {
        setDriverClassName(driverClassName);
        setUrl(url);
        setUsername(username);
        setPassword(password);
    }



    public void setDriverClassName(String driverClassName) {
        String driverClassNameToUse = driverClassName.trim();
        try {
            Class.forName(driverClassNameToUse, true, getDefaultClassLoader());
        }
        catch (ClassNotFoundException ex) {
            throw new IllegalStateException("Could not load JDBC driver class [" + driverClassNameToUse + "]", ex);
        }
    }


    public static ClassLoader getDefaultClassLoader() {
        ClassLoader cl = null;
        try {
            cl = Thread.currentThread().getContextClassLoader();
        }
        catch (Throwable ex) {
            // Cannot access thread context ClassLoader - falling back...
        }
        if (cl == null) {
            // No thread context class loader -> use class loader of this class.
            cl = DriverManagerDataSource.class.getClassLoader();
            if (cl == null) {
                // getClassLoader() returning null indicates the bootstrap ClassLoader
                try {
                    cl = ClassLoader.getSystemClassLoader();
                }
                catch (Throwable ex) {
                    // Cannot access system ClassLoader - oh well, maybe the caller can live with null...
                }
            }
        }
        return cl;
    }

    @Override
    protected Connection getConnectionFromDriver(Properties props) throws SQLException {
        String url = getUrl();

        return getConnectionFromDriverManager(url, props);
    }

    /**
     * Getting a Connection using the nasty static from DriverManager is extracted
     * into a protected method to allow for easy unit testing.
     *
     * @see java.sql.DriverManager#getConnection(String, java.util.Properties)
     */
    protected Connection getConnectionFromDriverManager(String url, Properties props) throws SQLException {
        return DriverManager.getConnection(url, props);
    }

}
