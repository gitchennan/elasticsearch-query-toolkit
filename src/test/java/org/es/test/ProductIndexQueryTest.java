package org.es.test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import org.elasticsearch.client.Client;
import org.es.jdbc.es.ElasticClientProvider;
import org.es.jdbc.es.ElasticClientProxyProviderImpl;
import org.es.mapping.mapper.MappingBuilder;
import org.es.spring.ElasticSqlMapClientTemplate;
import org.es.test.jdbc.BaseJdbcTest;
import org.es.test.jdbc.bean.Buyer;
import org.es.test.jdbc.bean.Product;
import org.es.test.jdbc.bean.Provider;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class ProductIndexQueryTest extends BaseJdbcTest {

    private ElasticSqlMapClientTemplate sqlMapClientTemplate;

    private static MappingBuilder mappingBuilder = new MappingBuilder();

    private static Client client;

    private static String setting = "{\n" +
            "    \"number_of_shards\": \"1\",\n" +
            "    \"number_of_replicas\": \"0\"\n" +
            "}";

    static {
        ElasticClientProvider clientProxyProvider = new ElasticClientProxyProviderImpl();
        client = clientProxyProvider.createElasticClientFromUrl(url);
    }

    @Before
    public void initForQueryTest() throws Exception {
        BeanFactory factory = new ClassPathXmlApplicationContext("application-context.xml");
        sqlMapClientTemplate = factory.getBean(ElasticSqlMapClientTemplate.class);

        if (client.admin().indices().prepareExists("index").execute().actionGet().isExists()) {
            client.admin().indices().prepareDelete("index").execute().actionGet();
        }

        client.admin().indices()
                .prepareCreate("index")
                .addMapping("product", mappingBuilder.buildMapping(Product.class).get("product").string())
                .setSettings(setting).execute().actionGet();

        Product product = new Product();
        product.setAdvicePrice(BigDecimal.valueOf(1000));
        product.setMinPrice(BigDecimal.valueOf(500));
        product.setProductCode("IP_6S");
        product.setProductName("iphone 6s");

        Provider provider = new Provider();
        provider.setProviderLevel(1);
        provider.setProviderName("FSK");

        product.setProvider(provider);

        Buyer china = new Buyer();
        china.setBuyerName("china");
        china.setProductPrice(2000);

        Buyer japan = new Buyer();
        japan.setBuyerName("japan");
        japan.setProductPrice(1800);

        product.setBuyers(Lists.newArrayList(china, japan));

        client.prepareIndex("index", "product", "1").setSource(new Gson().toJson(product, Product.class)).execute().actionGet();

        client.admin().indices().prepareRefresh("index").execute().actionGet();
    }

    @Test
    public void test_buildProductMapping() throws Exception {
        System.out.println(mappingBuilder.buildMappingAsString(Product.class));
    }

    public List<Product> getProductByCode(String productCode) throws SQLException {
        Map<String, Object> paramMap = Maps.newHashMap();
        paramMap.put("productCode", productCode);
        paramMap.put("advicePrice", 999);
        return sqlMapClientTemplate.queryForList("PRODUCT.getProductByCode", paramMap, Product.class);

    }

    public List<Product> getAllProduct() {
        return sqlMapClientTemplate.queryForList("PRODUCT.getAllProduct", Product.class);
    }


    @Test
    public void testProductQuery() throws Exception {
        Gson gson = new Gson();

        List<Product> productList = getProductByCode("IP_6S");
        for (Product product : productList) {
            System.out.println(gson.toJson(product));
        }

        System.out.println("============================>");

        productList = getAllProduct();
        for (Product product : productList) {
            System.out.println(gson.toJson(product));
        }
    }
}
