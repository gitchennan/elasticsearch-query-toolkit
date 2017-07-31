package org.es.mapping.bean;

public class Range<DataType> {

    DataType gte;

    DataType lte;

    public DataType getGte() {
        return gte;
    }

    public void setGte(DataType gte) {
        this.gte = gte;
    }

    public DataType getLte() {
        return lte;
    }

    public void setLte(DataType lte) {
        this.lte = lte;
    }
}
