package org.elasticsearch.jdbc.search;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.utils.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;

public class TransportClientProviderImpl implements TransportClientProvider {
    private static final int DEFAULT_ES_PORT = 9300;

    private static final Logger logger = LoggerFactory.getLogger(TransportClientProviderImpl.class);

    private static final String ELASTIC_SEARCH_DRIVER_PREFIX = "jdbc:elastic:";

    private final Map<String, TransportClient> clientMap = Maps.newHashMap();

    public TransportClientProviderImpl() {

    }

    public TransportClient createTransportClientFromUrl(final String url) {
        if (clientMap.containsKey(url)) {
            return clientMap.get(url);
        }

        try {
            TransportClient transportClient = internalBuildTransportClient(url);
            clientMap.put(url, transportClient);
        }
        catch (Exception ex) {
            logger.error(String.format("[TransportClientProviderImpl] Failed to build transport client for url[%s]", url), ex);
        }

        return clientMap.get(url);
    }

    private TransportClient internalBuildTransportClient(String url) {
        String ipUrl = url.substring(ELASTIC_SEARCH_DRIVER_PREFIX.length());

        Settings.Builder settingBuilder = Settings.settingsBuilder();
        settingBuilder.put("client.transport.sniff", true);

        String hostListString = ipUrl;
        int clusterNamePosIdx = ipUrl.lastIndexOf("/");
        if (clusterNamePosIdx >= 0) {
            hostListString = hostListString.substring(0, clusterNamePosIdx);
            settingBuilder.put("cluster.name", ipUrl.substring(clusterNamePosIdx + 1));
        }
        else {
            settingBuilder.put("client.transport.ignore_cluster_name", true);
        }

        List<InetSocketTransportAddress> addressList = Lists.newLinkedList();

        String[] connStringList = hostListString.split(Constants.COMMA);
        for (String connStr : connStringList) {
            String[] connArr = connStr.split(Constants.COLON);
            if (connArr.length == 1) {
                addressList.add(new InetSocketTransportAddress(new InetSocketAddress(connArr[0], DEFAULT_ES_PORT)));
            }
            else {
                addressList.add(new InetSocketTransportAddress(new InetSocketAddress(connArr[0], Integer.parseInt(connArr[1]))));
            }
        }

        return TransportClient.builder().settings(settingBuilder).build()
                .addTransportAddresses(addressList.toArray(new InetSocketTransportAddress[addressList.size()]));

    }
}
