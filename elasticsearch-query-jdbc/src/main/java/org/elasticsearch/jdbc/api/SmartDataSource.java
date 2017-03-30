package org.elasticsearch.jdbc.api;


import javax.sql.DataSource;
import java.sql.Connection;


public interface SmartDataSource extends DataSource {
    boolean shouldClose(Connection con);
}
