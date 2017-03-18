package org.elasticsearch.jdbc;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;

public class TransportClientFactory {

    private static final String COMMA = ",";
    private static final String COLON = ":";
    private static final int DEFAULT_ES_PORT = 9300;

    private static final Map<String, TransportClient> clientMap = Maps.newConcurrentMap();

    private TransportClientFactory() {

    }

    public static TransportClient createTransportClientFromUrl(String url) {
        if (clientMap.containsKey(url)) {
            clientMap.remove(url);
        }

        Settings.Builder settingBuilder = Settings.settingsBuilder();
        settingBuilder.put("client.transport.sniff", true);

        String hostListString = url;
        int clusterNamePosIdx = url.lastIndexOf("/");
        if (clusterNamePosIdx >= 0) {
            hostListString = hostListString.substring(0, clusterNamePosIdx);
            settingBuilder.put("cluster.name", url.substring(clusterNamePosIdx + 1));
        }
        else {
            settingBuilder.put("client.transport.ignore_cluster_name", true);
        }

        List<InetSocketTransportAddress> addressList = Lists.newLinkedList();

        String[] connStringList = hostListString.split(COMMA);
        for (String connStr : connStringList) {
            String[] connArr = connStr.split(COLON);
            if (connArr.length == 1) {
                addressList.add(new InetSocketTransportAddress(new InetSocketAddress(connArr[0], DEFAULT_ES_PORT)));
            }
            else {
                addressList.add(new InetSocketTransportAddress(new InetSocketAddress(connArr[0], Integer.parseInt(connArr[1]))));
            }
        }

        TransportClient transportClient = TransportClient.builder().settings(settingBuilder).build()
                .addTransportAddresses(addressList.toArray(new InetSocketTransportAddress[addressList.size()]));

        clientMap.put(url, transportClient);

        return clientMap.get(url);
    }
}
