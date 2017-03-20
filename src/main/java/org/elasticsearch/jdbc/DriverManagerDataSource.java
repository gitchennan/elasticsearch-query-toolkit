package org.elasticsearch.jdbc;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class DriverManagerDataSource extends AbstractDriverBasedDataSource {
    public DriverManagerDataSource() {
    }


    public DriverManagerDataSource(String url) {
        setUrl(url);
    }

    public DriverManagerDataSource(String url, Properties conProps) {
        setUrl(url);
        setConnectionProperties(conProps);
    }

    @Deprecated
    public DriverManagerDataSource(String driverClassName, String url) {
        setDriverClassName(driverClassName);
        setUrl(url);
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
        return getConnectionFromDriverManager(getUrl(), props);
    }

    protected Connection getConnectionFromDriverManager(String url, Properties props) throws SQLException {
        return DriverManager.getConnection(url, props);
    }

}
