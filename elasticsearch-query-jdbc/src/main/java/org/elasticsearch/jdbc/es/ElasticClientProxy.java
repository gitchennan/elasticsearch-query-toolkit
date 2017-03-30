package org.elasticsearch.jdbc.es;

import org.elasticsearch.client.Client;

public interface ElasticClientProxy extends Client {
    boolean isClosed();
}
