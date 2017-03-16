package org.elasticsearch.jdbc;

import java.sql.Connection;

public interface ConnectionProxy extends Connection {

    /**
     * Return the target Connection of this proxy.
     * <p>This will typically be the native driver Connection
     * or a wrapper from a connection pool.
     *
     * @return the underlying Connection (never {@code null})
     */
    Connection getTargetConnection();

}