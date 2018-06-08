package com.zendesk.maxwell.producer.kafka;

import com.zendesk.maxwell.api.config.CommandLineOptionParserContext;
import com.zendesk.maxwell.api.config.ConfigurationSupport;
import com.zendesk.maxwell.api.producer.ProducerConfiguration;
import com.zendesk.maxwell.api.producer.ProducerConfigurator;
import com.zendesk.maxwell.api.producer.ProducerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Enumeration;
import java.util.Optional;
import java.util.Properties;

@Service
public class KafkaProducerConfigurator implements ProducerConfigurator {

	private final ConfigurationSupport configurationSupport;

	@Autowired
	public KafkaProducerConfigurator(ConfigurationSupport configurationSupport) {
		this.configurationSupport = configurationSupport;
	}

	@Override
	public String getIdentifier() {
		return "kafka";
	}

	@Override
	public void configureCommandLineOptions(CommandLineOptionParserContext context) {
		context.addOptionWithRequiredArgument( "kafka_version", "kafka client library version: 0.8.2.2|0.9.0.1|0.10.0.1|0.10.2.1|0.11.0.1");
		context.addOptionWithRequiredArgument( "kafka_partition_by", "[deprecated]");
		context.addOptionWithRequiredArgument( "kafka_partition_columns", "[deprecated]");
		context.addOptionWithRequiredArgument( "kafka_partition_by_fallback", "[deprecated]");
		context.addOptionWithRequiredArgument( "kafka.bootstrap.servers", "at least one kafka server, formatted as HOST:PORT[,HOST:PORT]" );
		context.addOptionWithRequiredArgument( "kafka_partition_hash", "default|murmur3, hash function for partitioning" );
		context.addOptionWithRequiredArgument( "kafka_topic", "optionally provide a topic name to push to. default: maxwell" );
		context.addOptionWithRequiredArgument( "kafka_key_format", "how to format the kafka key; array|hash" );
	}

	@Override
	public Optional<ProducerConfiguration> parseConfiguration(Properties configurationValues) {
		KafkaProducerConfiguration config = new KafkaProducerConfiguration();
		config.setKafkaTopic(configurationSupport.fetchOption("kafka_topic", configurationValues, "maxwell"));
		config.setKafkaKeyFormat(configurationSupport.fetchOption("kafka_key_format", configurationValues, "hash"));
		config.setKafkaPartitionKey(configurationSupport.fetchOption("kafka_partition_by", configurationValues, null));
		config.setKafkaPartitionColumns(configurationSupport.fetchOption("kafka_partition_columns", configurationValues, null));
		config.setKafkaPartitionFallback(configurationSupport.fetchOption("kafka_partition_by_fallback", configurationValues, null));

		config.setKafkaPartitionHash(configurationSupport.fetchOption("kafka_partition_hash", configurationValues, "default"));
		config.setDdlKafkaTopic(configurationSupport.fetchOption("ddl_kafka_topic", configurationValues, config.getKafkaTopic()));

		String kafkaBootstrapServers = configurationSupport.fetchOption("kafka.bootstrap.servers", configurationValues, null);
		if (kafkaBootstrapServers != null){
			config.getKafkaProperties().setProperty("bootstrap.servers", kafkaBootstrapServers);
		}

		if (configurationValues != null) {
			for (Enumeration<Object> e = configurationValues.keys(); e.hasMoreElements(); ) {
				String k = (String) e.nextElement();
				if (k.startsWith("kafka.")) {
					if (k.equals("kafka.bootstrap.servers") && kafkaBootstrapServers != null){
						continue; // don't override command line bootstrap servers with config files'
					}
					config.getKafkaProperties().setProperty(k.replace("kafka.", ""), configurationValues.getProperty(k));
				}
			}
		}

		return Optional.of(config);
	}

	@Override
	public Class<? extends ProducerFactory> getFactory() {
		return KafkaProducerFactory.class;
	}

}
