package org.elasticsearch.jdbc;


import com.google.gson.Gson;
import org.junit.Test;

import java.sql.*;
import java.util.Enumeration;

public class ElasticDriverTest {
    private static final String driver = "org.elasticsearch.jdbc.ElasticDriver";
    private static final String url = "jdbc:elastic:192.168.0.109:9300/judge_cluster";

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
        ResultSet resultSet = connection.createStatement().executeQuery("select * from index.library where manager.managerName='lcy'");

        while (resultSet.next()) {
            String json = resultSet.getString(1);
            SearchResponseGson searchResponse = new Gson().fromJson(json, SearchResponseGson.class);
            System.out.println(searchResponse.getTookInMillis());
        }
    }

    @Test
    public void testQuery2() throws Exception {
        ElasticSingleConnectionDataSource dataSource = new ElasticSingleConnectionDataSource(url, true);
        dataSource.setDriverClassName(driver);

        Connection connection = dataSource.getConnection();
        String sql = "select * from index.library where (manager.managerName=? or manager.managerName=?)";

        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setString(1, "lcy");
        preparedStatement.setString(2, "chennan");

        ResultSet resultSet = preparedStatement.executeQuery();

        while (resultSet.next()) {
            String json = resultSet.getString(1);
            SearchResponseGson searchResponse = new Gson().fromJson(json, SearchResponseGson.class);
            System.out.println(searchResponse.getTookInMillis());
        }
    }
}
