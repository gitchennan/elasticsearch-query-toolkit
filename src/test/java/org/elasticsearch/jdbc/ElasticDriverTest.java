package org.elasticsearch.jdbc;


import org.elasticsearch.jdbc.api.ElasticConnection;
import org.elasticsearch.jdbc.api.ElasticSingleConnectionDataSource;
import org.elasticsearch.jdbc.bean.Product;
import org.elasticsearch.jdbc.bean.ProductAggResult;
import org.elasticsearch.jdbc.es.JdbcSearchResponse;
import org.elasticsearch.jdbc.es.JdbcSearchResponseResolver;
import org.junit.Test;

import java.sql.*;
import java.util.Enumeration;

public class ElasticDriverTest {
    private static final String driver = "org.elasticsearch.jdbc.api.ElasticDriver";
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
        ResultSet resultSet = connection.createStatement().executeQuery("select * from index.product where productCode='IP_6S'");

        String responseGson = resultSet.getString(1);
        JdbcSearchResponseResolver jdbcSearchResponseResolver = new JdbcSearchResponseResolver(responseGson);
        JdbcSearchResponse<Product> jdbcSearchResponse = jdbcSearchResponseResolver.resolveSearchResponse(Product.class);

        for (Product product : jdbcSearchResponse.getResultList()) {
            System.out.println(product.getProductName());
        }
    }

    @Test
    public void testQuery2() throws Exception {
        ElasticSingleConnectionDataSource dataSource = new ElasticSingleConnectionDataSource(url, true);
        dataSource.setDriverClassName(driver);

        Connection connection = dataSource.getConnection();
        String sql = "select * from index.product where productCode=? and provider.providerLevel > ?";

        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setString(1, "AW_OS2");
        preparedStatement.setInt(2, 0);

        ResultSet resultSet = preparedStatement.executeQuery();

        String responseGson = resultSet.getString(1);
        JdbcSearchResponseResolver jdbcSearchResponseResolver = new JdbcSearchResponseResolver(responseGson);
        JdbcSearchResponse<Product> jdbcSearchResponse = jdbcSearchResponseResolver.resolveSearchResponse(Product.class);

        for (Product product : jdbcSearchResponse.getResultList()) {
            System.out.println(product.getProductName());
        }
    }

    @Test
    public void testPrefixAndNestedQuery() throws Exception {
        ElasticSingleConnectionDataSource dataSource = new ElasticSingleConnectionDataSource(url, true);
        dataSource.setDriverClassName(driver);

        Connection connection = dataSource.getConnection();
        String sql = "select * from index.product where prefix(productName, 'iphone') and $buyers.productPrice > 1000";

        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        ResultSet resultSet = preparedStatement.executeQuery();

        String responseGson = resultSet.getString(1);
        JdbcSearchResponseResolver jdbcSearchResponseResolver = new JdbcSearchResponseResolver(responseGson);
        JdbcSearchResponse<Product> jdbcSearchResponse = jdbcSearchResponseResolver.resolveSearchResponse(Product.class);

        for (Product product : jdbcSearchResponse.getResultList()) {
            System.out.println(product.getProductName());
        }
    }

    @Test
    public void testGroupBy() throws Exception {
        ElasticSingleConnectionDataSource dataSource = new ElasticSingleConnectionDataSource(url, true);
        dataSource.setDriverClassName(driver);

        Connection connection = dataSource.getConnection();
        String sql = "select min(advicePrice),max(provider.providerLevel) from index.product group by terms(productCode)";

        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        ResultSet resultSet = preparedStatement.executeQuery();

        String responseGson = resultSet.getString(1);

        JdbcSearchResponseResolver jdbcSearchResponseResolver = new JdbcSearchResponseResolver(responseGson);
        JdbcSearchResponse<ProductAggResult> jdbcSearchResponse = jdbcSearchResponseResolver.resolveSearchResponse(ProductAggResult.class);

        System.out.println("resp total count: " + jdbcSearchResponse.getTotalCount());
        for (ProductAggResult aggItem : jdbcSearchResponse.getResultList()) {
            System.out.println(String.format("code:%s, count:%s, minPrice:%s, providerLevel:%s",
                    aggItem.getProductCode(), aggItem.getDocCount(), aggItem.getMinAdvicePrice(), aggItem.getProviderLevel()));
        }
    }


    @Test
    public void testScriptQuery() throws Exception {
        ElasticSingleConnectionDataSource dataSource = new ElasticSingleConnectionDataSource(url, true);
        dataSource.setDriverClassName(driver);

        Connection connection = dataSource.getConnection();
        String script = "if(doc[\"advicePrice\"].empty) return true; if(doc[\"minPrice\"].value/doc[\"advicePrice\"].value > 0.363) return true; else return false;";
        String sql = String.format("select * from index.product where script_query('%s')", script);

        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        ResultSet resultSet = preparedStatement.executeQuery();

        String responseGson = resultSet.getString(1);
        JdbcSearchResponseResolver jdbcSearchResponseResolver = new JdbcSearchResponseResolver(responseGson);
        JdbcSearchResponse<Product> jdbcSearchResponse = jdbcSearchResponseResolver.resolveSearchResponse(Product.class);

        for (Product product : jdbcSearchResponse.getResultList()) {
            System.out.println(String.format("productName:%s, minPrice:%s, advicePrice:%s", product.getProductName(), product.getMinPrice(), product.getAdvicePrice()));
        }
    }
}
