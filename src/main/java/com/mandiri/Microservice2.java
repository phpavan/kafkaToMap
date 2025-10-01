package com.mandiri;


import java.util.Map;

import com.hazelcast.internal.serialization.impl.compact.DeserializedGenericRecord;
import com.hazelcast.map.IMap;
import com.hazelcast.nio.serialization.genericrecord.GenericRecord;
import com.hazelcast.nio.serialization.genericrecord.GenericRecordBuilder;

public class Microservice2 {
    public static void main(String[] args) {
        CacheClient client = new CacheClient("microservice-2");
        
        
        SchemaRecord record = new SchemaRecord(4, "User1 in Schema-2 Data-4");

	     // Build GenericRecord
	     GenericRecord genericRecord = GenericRecordBuilder.compact("SchemaRecord")
	             .setInt32("id", record.getId())
	             .setString("description", record.getDescription())
	             .build();

       // Own cache (read + write)
       IMap<Integer, GenericRecord> schema2 = client.getOwnCache("schema2-cache");
       schema2.put(record.getId(), genericRecord);
       System.out.println("Schema write OK: " + schema2.get(4).getString("description"));
        

        // Read-only from Schema-2
        Map<Integer, DeserializedGenericRecord> schema1 = client.getReadOnlyCache("schema1-cache");
        System.out.println("Schema1 read-only: " + schema1.get(1).getString("description"));

        // Read-only from Schema-3
        Map<Integer, DeserializedGenericRecord> schema3 = client.getReadOnlyCache("schema3-cache");
        System.out.println("Schema3 read-only: " + schema3.get(1).getString("description"));
        
        client.shutdown();
    }
}
