package org.elasticsearch.dao;

import com.google.common.collect.Maps;
import org.elasticsearch.ElasticSqlMapExecutor;
import org.elasticsearch.bean.Product;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Repository;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

@Repository
public class ProductDao {
    @Autowired
    @Qualifier("sqlMapClientTemplate")
    private ElasticSqlMapExecutor sqlMapClientTemplate;

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
}
