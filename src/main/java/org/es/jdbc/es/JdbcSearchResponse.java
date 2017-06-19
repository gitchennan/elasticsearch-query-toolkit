package org.es.jdbc.es;

import com.google.gson.Gson;

import java.util.List;

public class JdbcSearchResponse<T> {
    private int totalShards;
    private int failedShards;
    private int successfulShards;
    private long tookInMillis;

    private long totalCount;
    private List<T> resultList;

    public int getTotalShards() {
        return totalShards;
    }

    public void setTotalShards(int totalShards) {
        this.totalShards = totalShards;
    }

    public int getFailedShards() {
        return failedShards;
    }

    public void setFailedShards(int failedShards) {
        this.failedShards = failedShards;
    }

    public int getSuccessfulShards() {
        return successfulShards;
    }

    public void setSuccessfulShards(int successfulShards) {
        this.successfulShards = successfulShards;
    }

    public long getTookInMillis() {
        return tookInMillis;
    }

    public void setTookInMillis(long tookInMillis) {
        this.tookInMillis = tookInMillis;
    }

    public List<T> getResultList() {
        return resultList;
    }

    public void setResultList(List<T> resultList) {
        this.resultList = resultList;
    }

    public long getTotalCount() {
        return totalCount;
    }

    public void setTotalCount(long totalCount) {
        this.totalCount = totalCount;
    }

    @Override
    public String toString() {
        return toJson();
    }

    public String toJson() {
        return new Gson().toJson(this, JdbcSearchResponse.class);
    }
}
