package org.elasticsearch.jdbc;


import org.elasticsearch.jdbc.api.ElasticSingleConnectionDataSource;
import org.elasticsearch.jdbc.bean.Product;
import org.elasticsearch.jdbc.es.JdbcSearchResponse;
import org.elasticsearch.jdbc.es.JdbcSearchResponseResolver;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class MethodQueryTest extends BaseJdbcTest {
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
}
