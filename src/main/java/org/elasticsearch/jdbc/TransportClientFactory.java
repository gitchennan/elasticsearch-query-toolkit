package org.elasticsearch.jdbc;

import com.google.common.collect.Maps;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.net.InetSocketAddress;
import java.util.Map;

public class TransportClientFactory {

    private static final String COMMA = ",";

    private static final Map<String, TransportClient> clientMap = Maps.newConcurrentMap();

    private TransportClientFactory() {

    }

    public static TransportClient createTransportClientFromUrl(String url) {
        if (clientMap.containsKey(url)) {
            return clientMap.get(url);
        }

        String[] connStringList = url.split(COMMA);
        for (String connStr : connStringList) {
            String[] connArr = connStr.split(":");
        }


        Settings settings = Settings.settingsBuilder()
                .put("cluster.name", "elasticsearch_wenbronk")
                .put("client.transport.sniff", true)
                .build();

        TransportClient transportClient = TransportClient.builder().settings(settings).build()
                .addTransportAddress(new InetSocketTransportAddress(new InetSocketAddress("192.168.50.37", 9300)));

        clientMap.put(url, transportClient);

        return clientMap.get(url);
    }
}
