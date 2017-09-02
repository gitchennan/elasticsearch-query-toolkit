package org.es.lu;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.es.mapping.utils.StringUtils;
import org.es.sql.utils.Constants;
import org.es.sql.utils.EsPersistLogger;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ElasticsearchCluster implements InitializingBean, DisposableBean {

    private static final String ELASTIC_SEARCH_DRIVER_PREFIX = "jdbc:elastic:";

    private CommonConfig commonConfig;

    private TransportClient client;

    private ClusterIndicesStateKeeper indicesStateKeeper;

    private String clusterKey;

    public ElasticsearchCluster(String url) {
        client = internalBuildElasticClient(url);
    }

    @Override
    public void destroy() throws Exception {
        indicesStateKeeper.stop();
        indicesStateKeeper = null;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        indicesStateKeeper = new ClusterIndicesStateKeeper(client);
        indicesStateKeeper.start();
    }

    public TransportClient client() {
        return client;
    }

    public boolean isClusterReadable() {
        return commonConfig.getClusterReadable(this.clusterKey);
    }

    public void setCommonConfig(CommonConfig commonConfig) {
        this.commonConfig = commonConfig;
    }

    public IndexState indexState(String indexName) {
        if (indicesStateKeeper.isIndicesStateCacheMissing()) {
            return IndexState.newIndexState(indexName, IndexState.IndexStatus.NO_NODE_AVAILABLE);
        }
        IndexState indexState = indicesStateKeeper.indicesStateCache().get(indexName);
        if (indexState == null) {
            indexState = IndexState.newIndexState(indexName, IndexState.IndexStatus.INDEX_MISSING);
        }
        return indexState;
    }

    public boolean isIndexWritable(String indexName) {
        if (StringUtils.isEmpty(indexName)) {
            return false;
        }
        IndexState indexState = indexState(indexName);
        return (IndexState.IndexStatus.GREEN == indexState.getIndexStatus()
                || IndexState.IndexStatus.YELLOW == indexState.getIndexStatus());
    }

    public boolean isIndicesWritable(String... indices) {
        if (indices == null || indices.length == 0) {
            return false;
        }
        for (String indexName : indices) {
            if (!isIndexWritable(indexName)) {
                return false;
            }
        }
        return true;
    }

    public String getClusterKey() {
        return clusterKey;
    }

    public void setClusterKey(String clusterKey) {
        this.clusterKey = clusterKey;
    }

    private TransportClient internalBuildElasticClient(String url) {
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
                addressList.add(new InetSocketTransportAddress(new InetSocketAddress(connArr[0], 9300)));
            }
            else {
                addressList.add(new InetSocketTransportAddress(new InetSocketAddress(connArr[0], Integer.parseInt(connArr[1]))));
            }
        }
        return new PreBuiltTransportClient(settingBuilder.build())
                .addTransportAddresses(addressList.toArray(new InetSocketTransportAddress[addressList.size()]));
    }

    private class ClusterIndicesStateKeeper implements Runnable {

        private TransportClient client;

        private volatile boolean indicesStateCacheMissing = false;

        private volatile ImmutableMap<String, IndexState> indicesStateCache;

        private ScheduledExecutorService executorService;

        public ClusterIndicesStateKeeper(TransportClient client) {
            this.client = client;
        }

        public void stop() throws Exception {
            executorService.shutdownNow();
            executorService = null;
        }

        public void start() throws Exception {
            indicesStateCache = newIndicesStateCacheInstance(client);
            executorService = Executors.newScheduledThreadPool(2);
            executorService.schedule(this, 3, TimeUnit.SECONDS);
        }

        public ImmutableMap<String, IndexState> indicesStateCache() {
            return indicesStateCache;
        }

        public boolean isIndicesStateCacheMissing() {
            return indicesStateCacheMissing;
        }

        @Override
        public void run() {
            indicesStateCache = newIndicesStateCacheInstance(client);
            if (!executorService.isShutdown()) {
                executorService.schedule(this, 10, TimeUnit.SECONDS);
            }
        }

        private ImmutableMap<String, IndexState> newIndicesStateCacheInstance(TransportClient client) {
            try {
                ImmutableMap.Builder<String, IndexState> cacheBuilder = ImmutableMap.<String, IndexState>builder();
                GetIndexResponse getIndexResponse = client.admin().indices().prepareGetIndex().execute().actionGet();

                ClusterHealthResponse clusterHealthResponse = client.admin().cluster().prepareHealth(getIndexResponse.indices()).execute().actionGet();
                for (String index : clusterHealthResponse.getIndices().keySet()) {
                    ClusterHealthStatus indexStatus = clusterHealthResponse.getIndices().get(index).getStatus();
                    cacheBuilder.put(index, IndexState.newIndexState(index, IndexState.IndexStatus.fromString(indexStatus.name())));
                }
                indicesStateCacheMissing = false;
                return cacheBuilder.build();
            }
            catch (Exception ex) {
                EsPersistLogger.warn(this, "Failed to get index state.", ex);
                indicesStateCacheMissing = true;
                return ImmutableMap.<String, IndexState>builder().build();
            }
        }
    }
}
