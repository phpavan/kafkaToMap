package com.kafka_hz;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import java.util.concurrent.atomic.AtomicLong;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.kafka.KafkaSources;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.shaded.org.json.JSONObject;


import java.util.Map;
import java.util.Properties;

import org.apache.kafka.common.serialization.StringDeserializer;

public class redhJson {
    public static void main(String[] args) {
        // Start Hazelcast Jet
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().addAddress("localhost:5701");
        HazelcastInstance instance = HazelcastClient.newHazelcastClient(clientConfig);

     

        // Kafka Configuration
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", "localhost:9092");
        kafkaProps.setProperty("key.deserializer", StringDeserializer.class.getName());
        kafkaProps.setProperty("value.deserializer", StringDeserializer.class.getName());
        kafkaProps.setProperty("auto.offset.reset", "earliest"); // Start from beginning
        kafkaProps.setProperty("group.id", "hazelcast-consumer-group");
        kafkaProps.setProperty("max.partition.fetch.bytes", "1024"); // Small batches
        kafkaProps.setProperty("fetch.max.wait.ms", "100"); // Low latency
        
        AtomicLong counter = new AtomicLong(1);
        
        // Define Pipeline
        Pipeline pipeline = Pipeline.create();
        pipeline.setPreserveOrder(true);
        pipeline.readFrom(KafkaSources.kafka(kafkaProps, "source"))
                .withNativeTimestamps(5)
                
                .map(record -> {
                	String id = (String)record.getKey();
                	System.out.println(" >>>>>>>>>>>>>>>>>>>>> "+id);
                	System.out.println(record.getValue());
                    String jsonString = record.getValue().toString();
                    JSONObject jsonObject = new JSONObject(jsonString);
                    //String id = jsonObject.optString("id", "unknown"); // Extract "id" field
                    long uniqueKey = counter.getAndIncrement();
                    return Map.entry(id, jsonString);
                    
                })
                .writeTo(Sinks.map("kafka-messages"));
               

        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("544");
        jobConfig.addClass(redhJson.class);
        
        // Execute Pipeline
        instance.getJet().newJob(pipeline, jobConfig).join();

        
    }
}

