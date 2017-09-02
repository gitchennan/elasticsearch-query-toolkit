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

    public ElasticsearchCluster[] getReadableClusters() {
        List<ElasticsearchCluster> clusterList = Lists.newArrayList();
        if (cluster01.isClusterReadable()) {
            clusterList.add(cluster01);
        }

        if (cluster02.isClusterReadable()) {
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

    public ElasticsearchCluster getReadableCluster() {
        ElasticsearchCluster[] clusters = getReadableClusters();
        if (clusters.length == 0) {
            return null;
        }
        if (clusters.length == 1) {
            return clusters[0];
        }

        int index = requestCount.incrementAndGet();
        if (index < 0) {
            index = 0;
            requestCount.set(index);
        }
        return clusters[index % clusters.length];
    }

}
