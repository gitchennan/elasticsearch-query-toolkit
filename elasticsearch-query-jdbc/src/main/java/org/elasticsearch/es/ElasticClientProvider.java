package org.elasticsearch.es;

import org.elasticsearch.client.Client;

public interface ElasticClientProvider {
    Client createElasticClientFromUrl(String url);
}
