package com.zendesk.maxwell.producer.kafka;

import com.zendesk.maxwell.api.config.InvalidOptionException;
import com.zendesk.maxwell.core.config.MaxwellConfig;
import com.zendesk.maxwell.core.producer.ProducerConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KafkaProducerConfiguration implements ProducerConfiguration {
	private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProducerConfiguration.class);

	public static final String DEFAULT_TOPIC = "maxwell";
	public static final String KEY_FORMAT_HASH = "hash";
	public static final String KEY_FORMAT_ARRAY = "array";
	public static final String PARTITION_HASH_DEFAULT = "default";
	public static final String PARTITION_HASH_MURMUR3 = "murmur3";

	public final Properties kafkaProperties = new Properties();
	public String kafkaTopic = DEFAULT_TOPIC;
	public String ddlKafkaTopic;
	public String kafkaKeyFormat = KEY_FORMAT_HASH;
	public String kafkaPartitionHash = PARTITION_HASH_DEFAULT;
	public String kafkaPartitionKey;
	public String kafkaPartitionColumns;
	public String kafkaPartitionFallback;

	@Override
	public void mergeWith(MaxwellConfig maxwellConfig) {
		if (maxwellConfig.producerPartitionKey == null && kafkaPartitionKey != null) {
			LOGGER.warn("kafka_partition_by is deprecated, please use producer_partition_by");
			maxwellConfig.producerPartitionKey = this.kafkaPartitionKey;
		}

		if (maxwellConfig.producerPartitionColumns == null && kafkaPartitionColumns != null) {
			LOGGER.warn("kafka_partition_columns is deprecated, please use producer_partition_columns");
			maxwellConfig.producerPartitionColumns = this.kafkaPartitionColumns;
		}

		if (maxwellConfig.producerPartitionFallback == null && kafkaPartitionFallback != null) {
			LOGGER.warn("kafka_partition_by_fallback is deprecated, please use producer_partition_by_fallback");
			maxwellConfig.producerPartitionFallback = this.kafkaPartitionFallback;
		}
	}

	@Override
	public void validate() {
		if ( !kafkaProperties.containsKey("bootstrap.servers") ) {
			throw new InvalidOptionException("You must specify kafka.bootstrap.servers for the kafka producer!", "kafka");
		}

		if ( kafkaPartitionHash == null ) {
			this.kafkaPartitionHash = PARTITION_HASH_DEFAULT;
		} else if ( !PARTITION_HASH_DEFAULT.equals(kafkaPartitionHash) && !PARTITION_HASH_MURMUR3.equals(kafkaPartitionHash) ) {
			throw new InvalidOptionException("please specify --kafka_partition_hash=default|murmur3", "kafka_partition_hash");
		}

		if ( !KEY_FORMAT_HASH.equals(kafkaKeyFormat) && !KEY_FORMAT_ARRAY.equals(kafkaKeyFormat) ){
			throw new InvalidOptionException("invalid kafka_key_format: " + this.kafkaKeyFormat, "kafka_key_format");
		}
	}
}
