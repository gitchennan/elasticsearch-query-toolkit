package org.elasticsearch.jdbc.search;

import org.elasticsearch.client.Client;

public interface ElasticClientProxy extends Client {
    boolean isClosed();
}
