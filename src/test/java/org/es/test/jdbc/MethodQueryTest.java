//package org.es.test.jdbc;
//
//
//import org.es.jdbc.api.ElasticSingleConnectionDataSource;
//import org.es.jdbc.es.JdbcSearchResponse;
//import org.es.jdbc.es.JdbcSearchResponseResolver;
//import org.es.test.jdbc.bean.Product;
//import org.junit.Test;
//
//import java.sql.Connection;
//import java.sql.PreparedStatement;
//import java.sql.ResultSet;
//
//public class MethodQueryTest extends BaseJdbcTest {
//    @Test
//    public void testPrefixAndNestedQuery() throws Exception {
//        ElasticSingleConnectionDataSource dataSource = new ElasticSingleConnectionDataSource(url, true);
//        dataSource.setDriverClassName(driver);
//
//        Connection connection = dataSource.getConnection();
//        String sql = "select * from index.product where prefix(productName, 'iphone') and $buyers.productPrice > 1000";
//
//        PreparedStatement preparedStatement = connection.prepareStatement(sql);
//        ResultSet resultSet = preparedStatement.executeQuery();
//
//        String responseGson = resultSet.getString(1);
//        JdbcSearchResponseResolver jdbcSearchResponseResolver = new JdbcSearchResponseResolver(responseGson);
//        JdbcSearchResponse<Product> jdbcSearchResponse = jdbcSearchResponseResolver.resolveSearchResponse(Product.class);
//
//        for (Product product : jdbcSearchResponse.getResultList()) {
//            System.out.println(product.getProductName());
//        }
//    }
//}
