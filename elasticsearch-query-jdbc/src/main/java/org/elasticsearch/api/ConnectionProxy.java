package org.elasticsearch.api;

import java.sql.Connection;

public interface ConnectionProxy extends Connection {
    Connection getTargetConnection();
}