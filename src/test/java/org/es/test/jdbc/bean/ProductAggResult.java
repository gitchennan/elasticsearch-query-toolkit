package org.es.test.jdbc.bean;

import com.google.gson.annotations.SerializedName;

import java.math.BigDecimal;

public class ProductAggResult {
    @SerializedName("_count")
    private Long docCount;
    @SerializedName("agg_productCode")
    private String productCode;
    @SerializedName("min_advicePrice")
    private BigDecimal minAdvicePrice;
    @SerializedName("max_provider.providerLevel")
    private Long providerLevel;

    public Long getDocCount() {
        return docCount;
    }

    public void setDocCount(Long docCount) {
        this.docCount = docCount;
    }

    public String getProductCode() {
        return productCode;
    }

    public void setProductCode(String productCode) {
        this.productCode = productCode;
    }

    public BigDecimal getMinAdvicePrice() {
        return minAdvicePrice;
    }

    public void setMinAdvicePrice(BigDecimal minAdvicePrice) {
        this.minAdvicePrice = minAdvicePrice;
    }

    public Long getProviderLevel() {
        return providerLevel;
    }

    public void setProviderLevel(Long providerLevel) {
        this.providerLevel = providerLevel;
    }
}
