package com.zendesk.maxwell.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class MaxwellKafkaFallBackProducer {
	private static final Logger LOGGER = LoggerFactory.getLogger(MaxwellKafkaFallBackProducer.class);

	private static MaxwellKafkaFallBackProducer fallBackProducer;

	public static MaxwellKafkaFallBackProducer getInstance(Properties kafkaProperties) {
		if (fallBackProducer == null) {
			synchronized (MaxwellKafkaFallBackProducer.class) {
				if (fallBackProducer == null) {
					fallBackProducer = new MaxwellKafkaFallBackProducer(kafkaProperties);
				}
			}
		}

		return fallBackProducer;
	}

	private KafkaProducer<String, String> kafkaProducer;
	private MaxwellKafkaFallBackProducer(Properties kafkaProperties) {
		kafkaProducer = new KafkaProducer<>(kafkaProperties, new StringSerializer(), new StringSerializer());
	}

	public void sendRecord(ProducerRecord<String, String> record) {
		kafkaProducer.send(record, new SimpleKafkaCallback(record.key()));
	}

	private class SimpleKafkaCallback implements Callback {
		private String recordKey;

		SimpleKafkaCallback(String recordKey) {
			this.recordKey = recordKey;
		}

		@Override
		public void onCompletion(RecordMetadata recordMetadata, Exception e) {
			if (e != null) {
				LOGGER.error("ERROR for record (" + recordKey + "): " + e.getLocalizedMessage());
			}
		}
	}
}
