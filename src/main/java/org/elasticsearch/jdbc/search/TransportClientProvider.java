package org.elasticsearch.jdbc.search;

import org.elasticsearch.client.transport.TransportClient;

public interface TransportClientProvider {
    TransportClient createTransportClientFromUrl(String url);
}
