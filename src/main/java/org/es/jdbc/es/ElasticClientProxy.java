package org.es.jdbc.es;

import org.elasticsearch.client.Client;

public interface ElasticClientProxy extends Client {
    boolean isClosed();
}
