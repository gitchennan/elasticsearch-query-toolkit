package org.elasticsearch.jdbc;


import org.junit.Test;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.util.Enumeration;

public class ElasticDriverTest {
    @Test
    public void testLoadDriver() throws Exception {
        Class.forName("org.elasticsearch.jdbc.ElasticDriver");

        Enumeration<Driver> driverEnumeration = DriverManager.getDrivers();

        while (driverEnumeration.hasMoreElements()) {
            Driver driver = driverEnumeration.nextElement();
            System.out.println(driver.toString());
        }
    }

    @Test
    public void testGetConnection() throws Exception {
        Class.forName("org.elasticsearch.jdbc.ElasticDriver");
        Connection connection = DriverManager.getConnection("jdbc:elastic:192.168.0.1:9200");
        org.junit.Assert.assertTrue(connection instanceof ElasticConnection);
    }
}
