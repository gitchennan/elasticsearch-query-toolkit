package org.es.test;

import com.google.common.collect.Maps;
import org.es.spring.ElasticSqlMapClientTemplate;
import org.es.test.bean.Product;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class ProductIndexQueryTest {

    private ElasticSqlMapClientTemplate sqlMapClientTemplate;

    @Before
    public void initSpringContext() {
        BeanFactory factory = new ClassPathXmlApplicationContext("application-context.xml");
        sqlMapClientTemplate = factory.getBean(ElasticSqlMapClientTemplate.class);
    }

    public List<Product> getProductByCodeAndMatchWord(String matchWord, String productCode) throws SQLException {
        Map<String, Object> paramMap = Maps.newHashMap();
        paramMap.put("productCode", productCode);
        paramMap.put("advicePrice", 1000);
        paramMap.put("routingVal", "A");
        paramMap.put("matchWord", matchWord);
        paramMap.put("prefixWord", matchWord);
        return sqlMapClientTemplate.queryForList("PRODUCT.getProductByCodeAndMatchWord", paramMap, Product.class);

    }

    public List<Product> getAllProduct() {
        return sqlMapClientTemplate.queryForList("PRODUCT.getAllProduct", Product.class);
    }


    @Test
    public void testProductQuery() throws Exception {
        List<Product> productList = getProductByCodeAndMatchWord("iphone 6s", "IP_6S");
        for (Product product : productList) {
            System.out.println(product.getProductName());
        }

        System.out.println("============================>");

        productList = getAllProduct();
        for (Product product : productList) {
            System.out.println(product.getProductName());
        }
    }
}
