package org.es.jdbc.es;

import org.elasticsearch.client.Client;

public interface ElasticClientProvider {
    Client createElasticClientFromUrl(String url);
}
