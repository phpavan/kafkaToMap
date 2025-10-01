package com.mandiri;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Objects;
import java.util.Properties;

import org.apache.kafka.common.serialization.StringDeserializer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.kafka.KafkaSources;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.pipeline.StreamStage;

public class HazelcastKafkaPipeline {

	// Postgres
	static final String PG_URL = System.getenv().getOrDefault("PG_URL",
			"jdbc:postgresql://localhost:5432/kwsp?user=ppavanku&password=ppavanku");
	static final String PG_USER = System.getenv().getOrDefault("PG_USER", "ppavanku");
	static final String PG_PASS = System.getenv().getOrDefault("PG_PASS", "ppavanku");

	static final String KAFKA_TOPIC = System.getenv().getOrDefault("KAFKA_TOPIC", "Mandiri");

	// Hazelcast data structures
	static final String MAP_CHANNEL_ACCOUNTS = "channel_accounts"; // key: account_number, value: CIF (or JSON)
	static final String MAP_ACCOUNT_BALANCE = "account_balance_cache"; // key: account_number, value: last balance JSON

	static final ObjectMapper mapper = new ObjectMapper();

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

		// To preserve order, we need to:
		// 1. Process one partition at a time
		// 2. Disable parallel processing for the source
		kafkaProps.setProperty("max.partition.fetch.bytes", "1024"); // Small batches
		kafkaProps.setProperty("fetch.max.wait.ms", "100"); // Low latency


		// Define Pipeline
		Pipeline pipeline = Pipeline.create();

		// 1) Read from Kafka
		StreamSource<String> source = KafkaSources.kafka(kafkaProps, record -> (String) record.value(), KAFKA_TOPIC);
		StreamStage<String> jsonStrings = pipeline.readFrom(source).withoutTimestamps();

		// 2) Parse to a simple POJO
		StreamStage<TxEvent> events = jsonStrings.map(json -> {
			try {
				return parse(mapper, json);
			} catch (Exception ex) {
				ex.printStackTrace();
				return null;
			}
		}).filter(Objects::nonNull);

		// 3) Keep only accounts that belong to "channel" users using an IMap lookup
		StreamStage<TxEvent> channelEvents = events
				.mapUsingIMap(MAP_CHANNEL_ACCOUNTS, TxEvent::getAccountNumber, (evt, cif) -> {
					TxEvent e = (TxEvent) evt; // cast ensures Jet knows type
					if (cif == null) {
						System.out.println("Dropped event: channel account mapping not found!");
						return null;
					}
					
		            JsonNode root = mapper.readTree(String.valueOf(cif));

		            // Navigate into channel_accounts -> channel_description
		            String channelDescription = root.path("channel_accounts")
		                                            .path("channel_description")
		                                            .asText();
					e.setCif(String.valueOf(channelDescription));
					return e;
				}).filter(e -> {
			        if (e == null) {
			            System.out.println("Dropped event: channel account mapping not found!");
			            return false; // filter out
			        }
			        return true;
			    });

		channelEvents.writeTo(Sinks.map("account_balance_map",
		        e -> e.getAccountNumber() + "|" + e.getCurrency(), // key
		        e -> e)); // value (TxEvent)

		JobConfig jobConfig = new JobConfig();
		jobConfig.addClass(HazelcastKafkaPipeline.class);
		// Execute Pipeline
		instance.getJet().newJob(pipeline, jobConfig).join();

	}

	// --- Helpers & model ---
	static TxEvent parse(ObjectMapper mapper, String json) throws Exception {
		JsonNode n = mapper.readTree(json);
		TxEvent e = new TxEvent();
		e.setSequence(n.path("sequence").asLong());
		e.setAccountNumber(n.path("trx_account").asText());
		e.setCif(n.path("cif").asText());

		e.setTrxType(n.path("trx_type").asText());
		e.setAmount(new BigDecimal(n.path("amount").asText()));
		e.setCurrency(n.path("currency").asText());
		e.setTrxDate(n.path("trx_date").asText());
		e.setRunningBalance(new BigDecimal(n.path("run_balance").asText()));
		e.setIngestTime(Instant.now().toString());
		return e;
	}

}
