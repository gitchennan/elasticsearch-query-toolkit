package org.elasticsearch.jdbc;


import javax.sql.DataSource;
import java.sql.Connection;


public interface SmartDataSource extends DataSource {
    boolean shouldClose(Connection con);
}
