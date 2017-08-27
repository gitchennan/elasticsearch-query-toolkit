//package org.es.test.jdbc;
//
//
//import org.es.jdbc.api.ElasticSingleConnectionDataSource;
//import org.es.jdbc.es.JdbcSearchResponse;
//import org.es.jdbc.es.JdbcSearchResponseResolver;
//import org.es.test.jdbc.bean.ProductAggResult;
//import org.junit.Test;
//
//import java.sql.Connection;
//import java.sql.PreparedStatement;
//import java.sql.ResultSet;
//
//public class GroupByTest extends BaseJdbcTest {
//
//    @Test
//    public void testGroupBy() throws Exception {
//        ElasticSingleConnectionDataSource dataSource = new ElasticSingleConnectionDataSource(url, true);
//        dataSource.setDriverClassName(driver);
//
//        Connection connection = dataSource.getConnection();
//        String sql = "select min(advicePrice),max(provider.providerLevel) from index.product group by terms(productCode, 200)";
//
//        PreparedStatement preparedStatement = connection.prepareStatement(sql);
//        ResultSet resultSet = preparedStatement.executeQuery();
//
//        String responseGson = resultSet.getString(1);
//
//        JdbcSearchResponseResolver jdbcSearchResponseResolver = new JdbcSearchResponseResolver(responseGson);
//        JdbcSearchResponse<ProductAggResult> jdbcSearchResponse = jdbcSearchResponseResolver.resolveSearchResponse(ProductAggResult.class);
//
//        System.out.println("resp total count: " + jdbcSearchResponse.getTotalCount());
//        for (ProductAggResult aggItem : jdbcSearchResponse.getResultList()) {
//            System.out.println(String.format("code:%s, count:%s, minPrice:%s, providerLevel:%s",
//                    aggItem.getProductCode(), aggItem.getDocCount(), aggItem.getMinAdvicePrice(), aggItem.getProviderLevel()));
//        }
//    }
//}
