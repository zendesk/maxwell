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
	public String topic = DEFAULT_TOPIC;
	public String ddlTopic;
	public String keyFormat = KEY_FORMAT_HASH;
	public String partitionHash = PARTITION_HASH_DEFAULT;
	public String partitionKey;
	public String partitionColumns;
	public String partitionFallback;

	@Override
	public void mergeWith(MaxwellConfig maxwellConfig) {
		if (maxwellConfig.producerPartitionKey == null && partitionKey != null) {
			LOGGER.warn("kafka_partition_by is deprecated, please use producer_partition_by");
			maxwellConfig.producerPartitionKey = this.partitionKey;
		}

		if (maxwellConfig.producerPartitionColumns == null && partitionColumns != null) {
			LOGGER.warn("kafka_partition_columns is deprecated, please use producer_partition_columns");
			maxwellConfig.producerPartitionColumns = this.partitionColumns;
		}

		if (maxwellConfig.producerPartitionFallback == null && partitionFallback != null) {
			LOGGER.warn("kafka_partition_by_fallback is deprecated, please use producer_partition_by_fallback");
			maxwellConfig.producerPartitionFallback = this.partitionFallback;
		}
	}

	@Override
	public void validate() {
		if ( !kafkaProperties.containsKey("bootstrap.servers") ) {
			throw new InvalidOptionException("You must specify kafka.bootstrap.servers for the kafka producer!", "kafka");
		}

		if ( partitionHash == null ) {
			this.partitionHash = PARTITION_HASH_DEFAULT;
		} else if ( !PARTITION_HASH_DEFAULT.equals(partitionHash) && !PARTITION_HASH_MURMUR3.equals(partitionHash) ) {
			throw new InvalidOptionException("please specify --kafka_partition_hash=default|murmur3", "kafka_partition_hash");
		}

		if ( !KEY_FORMAT_HASH.equals(keyFormat) && !KEY_FORMAT_ARRAY.equals(keyFormat) ){
			throw new InvalidOptionException("invalid kafka_key_format: " + this.keyFormat, "kafka_key_format");
		}
	}
}
