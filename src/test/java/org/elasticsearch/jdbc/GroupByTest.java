package org.elasticsearch.jdbc;


import org.elasticsearch.jdbc.api.ElasticSingleConnectionDataSource;
import org.elasticsearch.jdbc.bean.ProductAggResult;
import org.elasticsearch.jdbc.es.JdbcSearchResponse;
import org.elasticsearch.jdbc.es.JdbcSearchResponseResolver;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class GroupByTest {
    private static final String driver = "org.elasticsearch.jdbc.api.ElasticDriver";
    private static final String url = "jdbc:elastic:192.168.0.109:9300/lu-search-cluster";

    @Test
    public void testGroupBy() throws Exception {
        ElasticSingleConnectionDataSource dataSource = new ElasticSingleConnectionDataSource(url, true);
        dataSource.setDriverClassName(driver);

        Connection connection = dataSource.getConnection();
        String sql = "select min(advicePrice),max(provider.providerLevel) from index.product group by terms(productCode, 200)";

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
}
