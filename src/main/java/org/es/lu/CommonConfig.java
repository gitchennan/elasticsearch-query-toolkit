package org.es.lu;

import org.springframework.stereotype.Component;

@Component
public class CommonConfig {
    public boolean getClusterReadable(String clusterKey) {
        if ("BX".equalsIgnoreCase(clusterKey)) {
            return true;
        }

        if ("YP".equalsIgnoreCase(clusterKey)) {
            return false;
        }
        return false;
    }
}
