package com.zendesk.maxwell.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class MaxwellKafkaFallbackProducer {
	private static final Logger LOGGER = LoggerFactory.getLogger(MaxwellKafkaFallbackProducer.class);

	private static MaxwellKafkaFallbackProducer fallbackProducer;

	public static MaxwellKafkaFallbackProducer getInstance(Properties kafkaProperties) {
		if (fallbackProducer == null) {
			synchronized (MaxwellKafkaFallbackProducer.class) {
				if (fallbackProducer == null) {
					fallbackProducer = new MaxwellKafkaFallbackProducer(kafkaProperties);
				}
			}
		}

		return fallbackProducer;
	}

	private KafkaProducer<String, String> kafkaProducer;
	private MaxwellKafkaFallbackProducer(Properties kafkaProperties) {
		kafkaProducer = new KafkaProducer<>(kafkaProperties, new StringSerializer(), new StringSerializer());
	}

	public void sendRecord(ProducerRecord<String, String> record, AbstractAsyncProducer.CallbackCompleter cc) {
		kafkaProducer.send(record, new SimpleKafkaCallback(record.key(), cc));
	}

	private class SimpleKafkaCallback implements Callback {
		private String recordKey;
		private AbstractAsyncProducer.CallbackCompleter cc;

		SimpleKafkaCallback(String recordKey, AbstractAsyncProducer.CallbackCompleter cc) {
			this.recordKey = recordKey;
			this.cc = cc;
		}

		@Override
		public void onCompletion(RecordMetadata recordMetadata, Exception e) {
			if (e != null) {
				LOGGER.error("ERROR for record (" + recordKey + "): " + e.getLocalizedMessage());
			}
			cc.markCompleted();
		}
	}
}
