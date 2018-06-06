package com.zendesk.maxwell.core.producer.impl.kafka;

import com.zendesk.maxwell.core.producer.ProducerConfiguration;
import com.zendesk.maxwell.api.config.InvalidOptionException;
import com.zendesk.maxwell.api.config.MaxwellConfig;
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

	private final Properties kafkaProperties = new Properties();
	private String kafkaTopic = DEFAULT_TOPIC;
	private String ddlKafkaTopic;
	private String kafkaKeyFormat = KEY_FORMAT_HASH;
	private String kafkaPartitionHash = PARTITION_HASH_DEFAULT;
	private String kafkaPartitionKey;
	private String kafkaPartitionColumns;
	private String kafkaPartitionFallback;

	public Properties getKafkaProperties() {
		return kafkaProperties;
	}

	public String getKafkaTopic() {
		return kafkaTopic;
	}

	public void setKafkaTopic(String kafkaTopic) {
		this.kafkaTopic = kafkaTopic;
	}

	public String getDdlKafkaTopic() {
		return ddlKafkaTopic;
	}

	public void setDdlKafkaTopic(String ddlKafkaTopic) {
		this.ddlKafkaTopic = ddlKafkaTopic;
	}

	public String getKafkaKeyFormat() {
		return kafkaKeyFormat;
	}

	public void setKafkaKeyFormat(String kafkaKeyFormat) {
		this.kafkaKeyFormat = kafkaKeyFormat;
	}

	public String getKafkaPartitionHash() {
		return kafkaPartitionHash;
	}

	public void setKafkaPartitionHash(String kafkaPartitionHash) {
		this.kafkaPartitionHash = kafkaPartitionHash;
	}

	public String getKafkaPartitionKey() {
		return kafkaPartitionKey;
	}

	public void setKafkaPartitionKey(String kafkaPartitionKey) {
		this.kafkaPartitionKey = kafkaPartitionKey;
	}

	public String getKafkaPartitionColumns() {
		return kafkaPartitionColumns;
	}

	public void setKafkaPartitionColumns(String kafkaPartitionColumns) {
		this.kafkaPartitionColumns = kafkaPartitionColumns;
	}

	public String getKafkaPartitionFallback() {
		return kafkaPartitionFallback;
	}

	public void setKafkaPartitionFallback(String kafkaPartitionFallback) {
		this.kafkaPartitionFallback = kafkaPartitionFallback;
	}

	@Override
	public void mergeWith(MaxwellConfig maxwellConfig) {
		if (maxwellConfig.getProducerPartitionKey() == null && kafkaPartitionKey != null) {
			LOGGER.warn("kafka_partition_by is deprecated, please use producer_partition_by");
			maxwellConfig.setProducerPartitionKey(this.getKafkaPartitionKey());
		}

		if (maxwellConfig.getProducerPartitionColumns() == null && kafkaPartitionColumns != null) {
			LOGGER.warn("kafka_partition_columns is deprecated, please use producer_partition_columns");
			maxwellConfig.setProducerPartitionColumns(this.getKafkaPartitionColumns());
		}

		if (maxwellConfig.getProducerPartitionFallback() == null && kafkaPartitionFallback != null) {
			LOGGER.warn("kafka_partition_by_fallback is deprecated, please use producer_partition_by_fallback");
			maxwellConfig.setProducerPartitionFallback(this.getKafkaPartitionFallback());
		}
	}

	@Override
	public void validate() {
		if ( !kafkaProperties.containsKey("bootstrap.servers") ) {
			throw new InvalidOptionException("You must specify kafka.bootstrap.servers for the kafka producer!", "kafka");
		}

		if ( kafkaPartitionHash == null ) {
			this.setKafkaPartitionHash(PARTITION_HASH_DEFAULT);
		} else if ( !PARTITION_HASH_DEFAULT.equals(kafkaPartitionHash) && !PARTITION_HASH_MURMUR3.equals(kafkaPartitionHash) ) {
			throw new InvalidOptionException("please specify --kafka_partition_hash=default|murmur3", "kafka_partition_hash");
		}

		if ( !KEY_FORMAT_HASH.equals(kafkaKeyFormat) && !KEY_FORMAT_ARRAY.equals(kafkaKeyFormat) ){
			throw new InvalidOptionException("invalid kafka_key_format: " + this.getKafkaKeyFormat(), "kafka_key_format");
		}
	}
}
