package com.kafka_hz;


import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.kafka.KafkaSources;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.common.serialization.StringDeserializer;

public class HazelcastKafkaPipeline {
    public static void main(String[] args) {
        // Start Hazelcast Jet
    	 ClientConfig clientConfig = new ClientConfig();
    	    clientConfig.getNetworkConfig().addAddress("localhost:5701");
    	   
    		HazelcastInstance instance = HazelcastClient.newHazelcastClient(clientConfig);
    	
        Map<String, String> hazelcastMap = instance.getMap("kafka-data");

        // Kafka Configuration
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", "localhost:9092");
        kafkaProps.setProperty("key.deserializer", StringDeserializer.class.getName());
        kafkaProps.setProperty("value.deserializer", StringDeserializer.class.getName());
        kafkaProps.setProperty("auto.offset.reset", "earliest"); // Start from beginning
        kafkaProps.setProperty("group.id", "hazelcast-consumer-group");
        
        // To preserve order, we need to:
        // 1. Process one partition at a time
        // 2. Disable parallel processing for the source
        kafkaProps.setProperty("max.partition.fetch.bytes", "1024"); // Small batches
        kafkaProps.setProperty("fetch.max.wait.ms", "100"); // Low latency

        // Define Pipeline
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(KafkaSources.kafka(kafkaProps, "source"))
        .withNativeTimestamps(5)
        //.setLocalParallelism(1)// Important for order preservation
        
        .map(record -> {
        	//String key = record.toString();
            String value = record.toString();
        	System.out.println(">>>"+record);
             return Map.entry("my-key", value);
        })
        .writeTo(Sinks.map("kafka-messages"));

        JobConfig jobConfig = new JobConfig();
        jobConfig.addClass(HazelcastKafkaPipeline.class);
        // Execute Pipeline
    	instance.getJet().newJob(pipeline,jobConfig).join();

        // Print Hazelcast Map Content
        System.out.println("Hazelcast Map: " + hazelcastMap.size());
    }
}