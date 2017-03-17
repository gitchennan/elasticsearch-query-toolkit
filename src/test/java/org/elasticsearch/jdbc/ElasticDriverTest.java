package org.elasticsearch.jdbc;


import org.junit.Test;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.Enumeration;

public class ElasticDriverTest {
    private static final String driver = "org.elasticsearch.jdbc.ElasticDriver";
    private static final String url = "jdbc:elastic:192.168.0.1:9200";


    @Test
    public void testLoadDriver() throws Exception {
        Class.forName(driver);

        Enumeration<Driver> driverEnumeration = DriverManager.getDrivers();

        while (driverEnumeration.hasMoreElements()) {
            Driver driver = driverEnumeration.nextElement();
            System.out.println(driver.toString());
        }
    }

    @Test
    public void testGetConnection() throws Exception {
        Class.forName(driver);
        Connection connection = DriverManager.getConnection(url);
        org.junit.Assert.assertTrue(connection instanceof ElasticConnection);
    }

    @Test
    public void testDataSource() throws Exception {
        ElasticSingleConnectionDataSource dataSource = new ElasticSingleConnectionDataSource(url, false);
        dataSource.setDriverClassName(driver);

        Connection connection = dataSource.getConnection();
        org.junit.Assert.assertTrue(connection instanceof ElasticConnection);

        dataSource.destroy();
        org.junit.Assert.assertTrue(connection.isClosed());
    }

    @Test
    public void testQuery() throws Exception {
        ElasticSingleConnectionDataSource dataSource = new ElasticSingleConnectionDataSource(url, true);
        dataSource.setDriverClassName(driver);

        Connection connection = dataSource.getConnection();
        ResultSet resultSet = connection.createStatement().executeQuery("select * from reserve_record.rsr_plan_work_inst where id > 0");

        while(resultSet.next()) {
            String json = resultSet.getString(1);
            System.out.println(json);
        }
    }
}
