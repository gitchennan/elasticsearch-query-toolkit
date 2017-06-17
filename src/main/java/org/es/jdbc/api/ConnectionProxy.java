package org.es.jdbc.api;

import java.sql.Connection;

public interface ConnectionProxy extends Connection {
    Connection getTargetConnection();
}