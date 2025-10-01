package com.mandiri;


import java.util.Map;

import com.hazelcast.internal.serialization.impl.compact.DeserializedGenericRecord;
import com.hazelcast.map.IMap;
import com.hazelcast.nio.serialization.genericrecord.GenericRecord;
import com.hazelcast.nio.serialization.genericrecord.GenericRecordBuilder;

public class Microservice1 {
    public static void main(String[] args) {
        CacheClient client = new CacheClient("microservice-1");
        
        SchemaRecord record = new SchemaRecord(4, "User1 in Schema-1 Data-4");

	     // Build GenericRecord
	     GenericRecord genericRecord = GenericRecordBuilder.compact("SchemaRecord")
	             .setInt32("id", record.getId())
	             .setString("description", record.getDescription())
	             .build();

        // Own cache (read + write)
        IMap<Integer, GenericRecord> schema1 = client.getOwnCache("schema1-cache");
        schema1.put(record.getId(), genericRecord);
        System.out.println("Schema write OK: " + schema1.get(4).getString("description"));

        // Read-only from Schema-2
        Map<Integer, DeserializedGenericRecord> schema2 = client.getReadOnlyCache("schema2-cache");
        System.out.println("Schema2 read-only: " + schema2.get(1).getString("description"));

        // Read-only from Schema-3
        Map<Integer, DeserializedGenericRecord> schema3 = client.getReadOnlyCache("schema3-cache");
        System.out.println("Schema3 read-only: " + schema3.get(1).getString("description"));
        schema3.put(5,schema3.get(1));
       
        client.shutdown();
    }
}
