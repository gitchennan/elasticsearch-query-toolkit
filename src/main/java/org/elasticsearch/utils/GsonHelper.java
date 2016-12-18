package org.elasticsearch.utils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class GsonHelper {
    private GsonHelper() {

    }

    public static final Gson gson = new GsonBuilder().create();
}
