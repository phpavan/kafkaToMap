package com.kafka_hz;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import com.hazelcast.shaded.org.json.JSONObject;

import java.util.Properties;

public class Publisher {
    public static void main(String[] args) {
        // Kafka Producer Properties
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092"); // External Kafka Access
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        props.put("acks", "all");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        String topic = "source";

        // Simulated CDC events
        String[] events = new String[]{
                "{\"operation\":\"CREATE\", \"id\":\"123\", \"name\":\"1-John Doe\", \"email\":\"john.doe@example.com\"}",
                "{\"operation\":\"UPDATE\", \"id\":\"123\", \"name\":\"2-John A. Doe\", \"email\":\"john.doe@example.com\"}",
                "{\"operation\":\"CREATE\", \"id\":\"456\", \"name\":\"1-Bond\", \"email\":\"bond@example.com\"}",
                "{\"operation\":\"UPDATE\", \"id\":\"456\", \"name\":\"2-James Bond\", \"email\":\"james.bond@example.com\"}",
                "{\"operation\":\"UPDATE\", \"id\":\"123\", \"name\":\"3-John A. Doe\", \"email\":\"john.a.doe@example.com\"}",
                "{\"operation\":\"DELETE\", \"id\":\"123\"}",
                "{\"operation\":\"UPDATE\", \"id\":\"456\", \"name\":\"James Bond 007\", \"email\":\"james.bond.007@example.com\"}",
                "{\"operation\":\"DELETE\", \"id\":\"456\"}"
        };

        for (String event : events) {
        	
            JSONObject json = new JSONObject(event);
            String key = json.optString("id", "default-key"); // Extract 'id' as key or use default

            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, event);
            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    System.out.println("Sent to Partition: " + metadata.partition() + ", Offset: " + metadata.offset());
                } else {
                    exception.printStackTrace();
                }
            });
        }

        producer.close();
    }
}