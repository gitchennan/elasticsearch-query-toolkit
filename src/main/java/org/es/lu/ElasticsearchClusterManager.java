package org.es.lu;

import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@Component
public class ElasticsearchClusterManager {

    private AtomicInteger requestCount = new AtomicInteger(0);

    @Autowired
    @Qualifier("cluster01")
    private ElasticsearchCluster cluster01;

    @Autowired
    @Qualifier("cluster02")
    private ElasticsearchCluster cluster02;

    public ElasticsearchCluster[] getConfiguredActiveClusters() {
        List<ElasticsearchCluster> clusterList = Lists.newArrayList();
        if (cluster01.isConfiguredClusterActive()) {
            clusterList.add(cluster01);
        }

        if (cluster02.isConfiguredClusterActive()) {
            clusterList.add(cluster02);
        }
        return clusterList.toArray(new ElasticsearchCluster[clusterList.size()]);
    }

    public ElasticsearchCluster getCluster(String clusterKey) {
        if (cluster01.getClusterKey().equalsIgnoreCase(clusterKey)) {
            return cluster01;
        }

        if (cluster02.getClusterKey().equalsIgnoreCase(clusterKey)) {
            return cluster02;
        }
        return null;
    }

    public ElasticsearchCluster getReadableCluster(String... indices) {
        ElasticsearchCluster[] configuredActiveClusters = getConfiguredActiveClusters();
        if (configuredActiveClusters.length == 0 || indices == null || indices.length == 0) {
            return null;
        }

        List<ElasticsearchCluster> readableClusterList = Lists.newArrayList();
        for (ElasticsearchCluster configuredActiveCluster : configuredActiveClusters) {
            boolean isIndexReadable = true;
            for (String indexName : indices) {
                IndexState indexState = configuredActiveCluster.indexState(indexName);
                if (indexState.getIndexStatus() != IndexState.IndexStatus.GREEN
                        && indexState.getIndexStatus() != IndexState.IndexStatus.YELLOW) {
                    isIndexReadable = false;
                    break;
                }
            }

            if (isIndexReadable) {
                readableClusterList.add(configuredActiveCluster);
            }
        }

        if (readableClusterList.size() == 0) {
            return null;
        }
        if (readableClusterList.size() == 1) {
            return readableClusterList.get(0);
        }

        int index = requestCount.incrementAndGet();
        if (index < 0) {
            index = 0;
            requestCount.set(index);
        }
        return readableClusterList.get(index % readableClusterList.size());
    }
}
