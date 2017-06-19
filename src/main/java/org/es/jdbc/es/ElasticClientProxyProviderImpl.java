package org.es.jdbc.es;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.es.jdbc.exception.BuildElasticClientException;
import org.es.sql.utils.Constants;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;

public class ElasticClientProxyProviderImpl implements ElasticClientProvider {
    private static final int DEFAULT_ES_PORT = 9300;
    private static final String ELASTIC_SEARCH_DRIVER_PREFIX = "jdbc:elastic:";

    private final Map<String, ElasticClientProxy> clientMap = Maps.newHashMap();

    public ElasticClientProxyProviderImpl() {

    }

    public Client createElasticClientFromUrl(final String url) {
        if (clientMap.containsKey(url)) {
            return clientMap.get(url);
        }

        try {
            ElasticClientProxy elasticClient = internalBuildElasticClient(url);
            clientMap.put(url, elasticClient);
        }
        catch (Exception ex) {
            throw new BuildElasticClientException(String.format("Failed to build transport client for url[%s]", url), ex);
        }

        return clientMap.get(url);
    }

    private ElasticClientProxy internalBuildElasticClient(String url) {
        String ipUrl = url.substring(ELASTIC_SEARCH_DRIVER_PREFIX.length());

        Settings.Builder settingBuilder = Settings.builder();
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


        TransportClient transportClient = new PreBuiltTransportClient(settingBuilder.build())
                .addTransportAddresses(addressList.toArray(new InetSocketTransportAddress[addressList.size()]));

        return (ElasticClientProxy) Proxy.newProxyInstance(ElasticClientProxy.class.getClassLoader(),
                new Class[]{ElasticClientProxy.class}, new CloseClientProxyInvocationHandler(transportClient));

    }


    private class CloseClientProxyInvocationHandler implements InvocationHandler {
        private volatile boolean isClosed = false;
        private Object target;

        public CloseClientProxyInvocationHandler(Client client) {
            this.target = client;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            if ("close".equals(method.getName())) {
                isClosed = true;
            }
            else if ("isClosed".equals(method.getName())) {
                return isClosed;
            }

            try {
                return method.invoke(target, args);
            }
            catch (InvocationTargetException ex) {
                throw ex.getTargetException();
            }
        }
    }

}
