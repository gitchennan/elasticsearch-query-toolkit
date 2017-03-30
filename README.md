elasticsearch-query-tookit
====================================

版本
-------------

toolkit version | ES version
-----------|-----------
master | 2.4.4
2.4.4.1 | 2.4.4
1.x    | 1.4.5

介绍
-------------
> `elasticsearch-query-tookit`是一款基于SQL查询elasticsearch编程工具包，支持SQL解析生成DSL，支持JDBC驱动，支持和Spring、MyBatis集成，提供Java编程接口可基于此工具包二次开发


## 一、SQL解析生成DSL使用示例

**[SQL语法帮助手册戳这里: https://github.com/gitchennan/elasticsearch-query-toolkit/wiki/elasticsearch-query-toolkit-help-doc](https://github.com/gitchennan/elasticsearch-query-toolkit/wiki/elasticsearch-query-toolkit-help-doc)**
```java
String sql = "select * from index.order where status='SUCCESS' and price > 100 order by nvl(pride, 0) asc routing by 'JD' limit 0, 20";

ElasticSql2DslParser sql2DslParser = new ElasticSql2DslParser();
//解析SQL
ElasticSqlParseResult parseResult = sql2DslParser.parse(sql);
//生成DSL(可用于rest api调用)
String dsl = parseResult.toDsl();

//toRequest方法接收一个clinet对象参数
SearchRequestBuilder searchReq = parseResult.toRequest(esClient);
//执行查询
SearchResponse response = searchReq.execute().actionGet();
```
生成的DSL如下：
```bash
{
  "from" : 0,
  "size" : 20,
  "query" : {
    "bool" : {
      "filter" : {
        "bool" : {
          "must" : [ {
            "term" : {
              "status" : "SUCCESS"
            }
          }, {
            "range" : {
              "price" : {
                "from" : 100,
                "to" : null,
                "include_lower" : false,
                "include_upper" : true
              }
            }
          } ]
        }
      }
    }
  },
  "sort" : [ {
    "pride" : {
      "order" : "asc",
      "missing" : 0
    }
  } ]
}
```

## 二、集成MyBatis、Spring
首先在Spring配置文件中增加如下代码
1. 指定driverClassName：org.elasticsearch.jdbc.api.ElasticDriver
2. 指定连接ES的连接串：jdbc:elastic:192.168.0.109:9300/product_cluster
3. 创建一个SqlMapClient对象，并指定sqlMapConfig.xml路径
```bash
<bean id="elasticDataSource" class="org.elasticsearch.jdbc.api.ElasticSingleConnectionDataSource" destroy-method="destroy">
    <property name="driverClassName" value="org.elasticsearch.jdbc.api.ElasticDriver" />
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
        
        //反序列化查询结果
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

