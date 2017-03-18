package org.elasticsearch.jdbc;

import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.util.List;

public class SearchResponseGson {
    private int totalShards;
    private int failedShards;
    private int successfulShards;
    private long tookInMillis;
    private long totalHits;

    private List<String> docList;


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

    public <T> List<T> getDocList(TypeToken<T> typeToken) {
        List<T> docs = Lists.newLinkedList();
        Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ").create();
        for (String docJson : docList) {
            docs.add(gson.fromJson(docJson, typeToken.getType()));
        }
        return docs;
    }

    public void setDocList(List<String> docList) {
        this.docList = docList;
    }

    @Override
    public String toString() {
        return toJson();
    }

    public String toJson() {
        return new Gson().toJson(this, SearchResponseGson.class);
    }

}
