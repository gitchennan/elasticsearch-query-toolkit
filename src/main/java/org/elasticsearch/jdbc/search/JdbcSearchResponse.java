package org.elasticsearch.jdbc.search;

import com.google.gson.Gson;

import java.util.List;

public class JdbcSearchResponse<T> {
    private int totalShards;
    private int failedShards;
    private int successfulShards;
    private long tookInMillis;
    private long totalHits;

    private List<T> docList;

    public int getTotalShards() {
        return totalShards;
    }

    public long getTotalHits() {
        return totalHits;
    }

    public void setTotalHits(long totalHits) {
        this.totalHits = totalHits;
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

    public List<T> getDocList() {
        return docList;
    }

    public void setDocList(List<T> docList) {
        this.docList = docList;
    }

    @Override
    public String toString() {
        return toJson();
    }

    public String toJson() {
        return new Gson().toJson(this, JdbcSearchResponse.class);
    }

}
