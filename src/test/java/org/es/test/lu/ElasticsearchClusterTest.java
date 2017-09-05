package org.es.test.lu;

import org.es.lu.ElasticsearchCluster;
import org.es.lu.ElasticsearchClusterManager;
import org.es.lu.IndexState;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.util.concurrent.TimeUnit;

public class ElasticsearchClusterTest {

    private ClassPathXmlApplicationContext applicationContext;

    private ElasticsearchClusterManager clusterManager;

    @Before
    public void initSpringContext() {
        applicationContext = new ClassPathXmlApplicationContext("application-lu.xml");
        clusterManager = (ElasticsearchClusterManager) applicationContext.getBean("elasticsearchClusterManager");
    }

    @Test
    @Ignore
    public void test_initCluster() throws Exception {
        for (int i = 0; i < 1; i++) {
            ElasticsearchCluster cluster = clusterManager.getCluster("BX");
            IndexState indexState = cluster.indexState("index");
            System.out.println(cluster.getClusterKey() + " ===========>" + indexState);

            cluster = clusterManager.getCluster("YP");
            indexState = cluster.indexState(".custom-dictionary");
            System.out.println(cluster.getClusterKey() + " ===========>" + indexState);

            TimeUnit.SECONDS.sleep(2);
        }
    }

    @Test
    @Ignore
    public void test_getReadableCluster() throws Exception {
        for (int i = 0; i < 1; i++) {
            ElasticsearchCluster cluster = clusterManager.getReadableCluster(".custom-dictionary");
            if (cluster == null) {
                System.out.println("cluster is null, continue.");
                TimeUnit.SECONDS.sleep(1);
                continue;
            }
            System.out.println(cluster.getClusterKey());
            TimeUnit.SECONDS.sleep(1);
        }
    }

    @Test
    public void test_copyMetadata() {
        ElasticsearchCluster cluster = clusterManager.getCluster("BX");
    }
}
