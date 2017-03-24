package org.elasticsearch.jdbc;


import org.elasticsearch.dsl.bean.ElasticSqlParseResult;
import org.elasticsearch.dsl.parser.ElasticSql2DslParser;
import org.elasticsearch.jdbc.api.ElasticSingleConnectionDataSource;
import org.elasticsearch.jdbc.bean.Product;
import org.elasticsearch.jdbc.es.JdbcSearchResponse;
import org.elasticsearch.jdbc.es.JdbcSearchResponseResolver;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class ScriptQueryTest {
    private static final String driver = "org.elasticsearch.jdbc.api.ElasticDriver";
    private static final String url = "jdbc:elastic:192.168.0.109:9300/lu-search-cluster";

    @Test
    public void testScriptQuery() throws Exception {
        ElasticSingleConnectionDataSource dataSource = new ElasticSingleConnectionDataSource(url, true);
        dataSource.setDriverClassName(driver);

        Connection connection = dataSource.getConnection();

        String script = "if(doc[\"advicePrice\"].empty) return false; if(my_var * doc[\"minPrice\"].value/doc[\"advicePrice\"].value > 0.363) return true; else return false;";
        String sql = String.format("select * from index.product where script_query('%s', 'my_var:2.1f')", script);

        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        ResultSet resultSet = preparedStatement.executeQuery();

        String responseGson = resultSet.getString(1);
        JdbcSearchResponseResolver jdbcSearchResponseResolver = new JdbcSearchResponseResolver(responseGson);
        JdbcSearchResponse<Product> jdbcSearchResponse = jdbcSearchResponseResolver.resolveSearchResponse(Product.class);

        for (Product product : jdbcSearchResponse.getResultList()) {
            System.out.println(String.format("productName:%s, minPrice:%s, advicePrice:%s", product.getProductName(), product.getMinPrice(), product.getAdvicePrice()));
        }


//        sql = "";
//        ElasticSql2DslParser sql2DslParser = new ElasticSql2DslParser();
//        ElasticSqlParseResult parseResult = sql2DslParser.parse(sql);
//        String dsl = parseResult.toDsl();
//        System.out.println(dsl);
    }
}
