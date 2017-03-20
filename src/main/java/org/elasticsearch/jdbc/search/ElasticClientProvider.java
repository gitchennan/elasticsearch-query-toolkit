package org.elasticsearch.jdbc.search;

import org.elasticsearch.client.Client;

public interface ElasticClientProvider {
    Client createElasticClientFromUrl(String url);
}
