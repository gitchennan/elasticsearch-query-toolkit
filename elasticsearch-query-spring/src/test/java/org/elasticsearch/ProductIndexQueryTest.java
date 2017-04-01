package org.elasticsearch;

import org.elasticsearch.bean.Product;
import org.elasticsearch.dao.ProductDao;
import org.junit.Test;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.util.List;

public class ProductIndexQueryTest {

    @Test
    public void testProductQuery() throws Exception {
        BeanFactory factory = new ClassPathXmlApplicationContext("application-context.xml");

        ProductDao productDao = factory.getBean(ProductDao.class);

        List<Product> productList = productDao.getProductByCodeAndMatchWord("iphone 6s", "IP_6S");
        for (Product product : productList) {
            System.out.println(product.getProductName());
        }

        System.out.println("============================>");

        productList = productDao.getAllProduct();
        for (Product product : productList) {
            System.out.println(product.getProductName());
        }
    }
}
