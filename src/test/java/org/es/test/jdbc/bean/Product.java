package org.es.test.jdbc.bean;

import org.es.mapping.annotations.TypeSetting;
import org.es.mapping.annotations.enums.StringType;
import org.es.mapping.annotations.fieldtype.StringField;

import java.math.BigDecimal;
import java.util.List;

@TypeSetting(_type = "product")
public class Product {

    private String productName;

    @StringField(type = StringType.Keyword)
    private String productCode;

    private BigDecimal minPrice;

    private BigDecimal advicePrice;

    private Provider provider;

    private List<Buyer> buyers;

    public String getProductName() {
        return productName;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    public String getProductCode() {
        return productCode;
    }

    public void setProductCode(String productCode) {
        this.productCode = productCode;
    }

    public BigDecimal getMinPrice() {
        return minPrice;
    }

    public void setMinPrice(BigDecimal minPrice) {
        this.minPrice = minPrice;
    }

    public BigDecimal getAdvicePrice() {
        return advicePrice;
    }

    public void setAdvicePrice(BigDecimal advicePrice) {
        this.advicePrice = advicePrice;
    }

    public Provider getProvider() {
        return provider;
    }

    public void setProvider(Provider provider) {
        this.provider = provider;
    }

    public List<Buyer> getBuyers() {
        return buyers;
    }

    public void setBuyers(List<Buyer> buyers) {
        this.buyers = buyers;
    }
}
