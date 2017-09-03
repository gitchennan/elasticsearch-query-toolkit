package org.es.lu;

import org.junit.Before;
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
    public void test_initCluster() throws Exception {
        for (int i = 0; i < 5000; i++) {
            ElasticsearchCluster cluster = clusterManager.getCluster("YP");
            IndexState indexState = cluster.indexState("index");
            System.out.println(cluster.getClusterKey() + " ===========>" + indexState);

//            cluster = clusterManager.getCluster("BX");
//            indexState = cluster.indexState(".custom-dictionary");
//            System.out.println(cluster.getClusterKey() + " ===========>" + indexState);

            TimeUnit.SECONDS.sleep(2);
        }
    }

    @Test
    public void test_getReadableCluster() throws Exception {
        for (int i = 0; i < 500; i++) {
            ElasticsearchCluster cluster = clusterManager.getReadableCluster("index");
            System.out.println(cluster.getClusterKey());

            TimeUnit.SECONDS.sleep(1);
        }
    }

    @Test
    public void test_copyMetadata() {
        ElasticsearchCluster cluster = clusterManager.getCluster("BX");
    }
}

