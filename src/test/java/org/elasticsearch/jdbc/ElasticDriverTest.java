package org.elasticsearch.jdbc;


import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.junit.Test;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.Enumeration;
import java.util.List;

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

        while(resultSet.next()) {
            String json = resultSet.getString(1);
            SearchResponseGson searchResponse = new Gson().fromJson(json, SearchResponseGson.class);

            System.out.println(searchResponse.getTookInMillis());

            List<Lib> libList = searchResponse.getDocList(new TypeToken<Lib>(){});

            for (Lib lib : libList) {
                System.out.println(lib.getName());
            }
        }
    }
}

class Lib {
    private String name;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
