package com.mandiri;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.*;
import com.hazelcast.map.IMap;

import java.util.Collections;
import java.util.Map;

public class CacheClient {
    private final HazelcastInstance client;

    public CacheClient(String microserviceName) {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().addAddress("localhost:5701");
        clientConfig.setInstanceName(microserviceName + "-client");
       
       

        this.client = HazelcastClient.newHazelcastClient(clientConfig);
    }

    // Full access for own schema
    public <K, V> IMap<K, V> getOwnCache(String schemaName) {
        return client.getMap(schemaName);
    }

    // Read-only wrapper for other schemas
    public <K, V> Map<K, V> getReadOnlyCache(String schemaName) {
        IMap<K, V> map = client.getMap(schemaName);
        return Collections.unmodifiableMap(map);
    }

    public void shutdown() {
        client.shutdown();
    }
}
