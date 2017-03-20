elasticsearch-query-tookit
====================================

版本
-------------

sql2dsl version | ES version
-----------|-----------
master | 2.4.4
2.x    | 2.4.4
1.x    | 1.4.5

介绍
-------------
> `elasticsearch-query-tookit`是一款elasticsearch查询编程工具包，提供Java编程接口，支持SQL解析生成DSL，支持JDBC驱动，支持和spring、ibatis集成


## 一、SQL解析生成DSL使用示例


```java
String sql = "select * from index.order where status='SUCCESS' order by nvl(pride, 0) asc routing by 'JD' limit 0, 20";

ElasticSql2DslParser sql2DslParser = new ElasticSql2DslParser();
//解析SQL
ElasticSqlParseResult parseResult = sql2DslParser.parse(sql);
//生成DSL,可用于rest api调用
String dsl = parseResult.toDsl();

//toRequest方法接收一个clinet对象参数，用于生成SearchRequestBuilder
SearchRequestBuilder searchReq = parseResult.toRequest(esClient);
//执行查询
SearchResponse response = searchReq.execute().actionGet();
```


## 二、集成IBatis、Spring
首先在Spring配置文件中增加如下代码
1. 指定driverClassName：org.elasticsearch.jdbc.ElasticDriver
2. 指定连接ES的连接串：jdbc:elastic:192.168.0.109:9300/product_cluster
3. 创建一个SqlMapClient对象，并指定sqlMapConfig.xml路径
```bash
<bean id="elasticDataSource" class="org.elasticsearch.jdbc.ElasticSingleConnectionDataSource" destroy-method="destroy">
    <property name="driverClassName" value="org.elasticsearch.jdbc.ElasticDriver" />
    <property name="url" value="jdbc:elastic:192.168.0.109:9300/product_cluster" />
</bean>

<bean id="sqlMapClient" class="org.springframework.orm.ibatis.SqlMapClientFactoryBean">
    <property name="dataSource" ref="elasticDataSource" />
    <property name="configLocation" value="classpath:sqlMapConfig.xml"/>
</bean>
```

sqlMapConfig.xml文件内容如下：
```bash
<sqlMapConfig>
    <settings
            cacheModelsEnabled="true"
            lazyLoadingEnabled="true"
            enhancementEnabled="true"
            maxSessions="64"
            maxTransactions="20"
            maxRequests="128"
            useStatementNamespaces="true"/>

    <sqlMap resource="sqlmap/PRODUCT.xml"/>

</sqlMapConfig>
```

PRODUCT.xml文件中声明select sql语句
```bash
<sqlMap namespace="PRODUCT">
    <select id="getProductByCodeAndMatchWord" parameterClass="java.util.Map" resultClass="java.lang.String">
        SELECT *
        FROM index.product
        QUERY match(productName, #matchWord#) or prefix(productName, #prefixWord#, 'boost:2.0f')
        WHERE productCode = #productCode#
        AND advicePrice > #advicePrice#
        AND $$buyers.buyerName IN ('china', 'usa')
        ROUTING BY #routingVal#
    </select>
</sqlMap>
```


编写对应DAO代码：
```bash
@Repository
public class ProductDao {
    @Autowired
    @Qualifier("sqlMapClient")
    private SqlMapClient sqlMapClient;


    public List<Product> getProductByCodeAndMatchWord(String matchWord, String productCode) throws SQLException {
        Map<String, Object> paramMap = Maps.newHashMap();
        paramMap.put("productCode", productCode);
        paramMap.put("advicePrice", 1000);
        paramMap.put("routingVal", "A");
        paramMap.put("matchWord", matchWord);
        paramMap.put("prefixWord", matchWord);
        String responseGson = (String) sqlMapClient.queryForObject("PRODUCT.getProductByCodeAndMatchWord", paramMap);

        JdbcSearchResponseResolver responseResolver = new JdbcSearchResponseResolver(responseGson);
        JdbcSearchResponse<Product> searchResponse = responseResolver.resolveSearchResponse(Product.class);

        return searchResponse.getDocList();

    }
}
```
编写测试方法
```bash
@Test
public void testProductQuery() throws Exception {
    BeanFactory factory = new ClassPathXmlApplicationContext("application-context.xml");

    ProductDao productDao = factory.getBean(ProductDao.class);

    List<Product> productList = productDao.getProductByCodeAndMatchWord("iphone 6s", "IP_6S");

    for (Product product : productList) {
        System.out.println(product.getProductName());
    }
}
```
作者:  [@陈楠][1]
Email: 465360798@qq.com

<完>

