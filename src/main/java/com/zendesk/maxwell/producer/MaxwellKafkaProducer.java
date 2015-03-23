package com.zendesk.maxwell.producer;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Properties;

import com.zendesk.maxwell.BinlogPosition;
import com.zendesk.maxwell.MaxwellAbstractRowsEvent;
import com.zendesk.maxwell.MaxwellConfig;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class KafkaCallback implements Callback {
	static final Logger LOGGER = LoggerFactory.getLogger(MaxwellKafkaProducer.class);
	private final MaxwellConfig config;
	private final MaxwellAbstractRowsEvent event;
	public KafkaCallback(MaxwellAbstractRowsEvent e, MaxwellConfig c) {
		this.config = c;
		this.event = e;
	}

	@Override
	public void onCompletion(RecordMetadata md, Exception e) {
		if ( e != null ) {
			e.printStackTrace();
		} else {
			try {
				BinlogPosition p = new BinlogPosition(event.getHeader().getNextPosition(), event.getBinlogFilename());
				if ( LOGGER.isDebugEnabled()) {
					LOGGER.debug("-> " + md.topic() + ":" + md.offset());
					LOGGER.debug("   " + event.toJSON());
					LOGGER.debug("   " + p);
					LOGGER.debug("");
				}
				config.setInitialPosition(p);
			} catch (IOException | SQLException e1) {
				e1.printStackTrace();
			}
		}
	}
}
public class MaxwellKafkaProducer extends AbstractProducer {
	static final Logger LOGGER = LoggerFactory.getLogger(MaxwellKafkaProducer.class);
	private final KafkaProducer<byte[], byte[]> kafka;

	public MaxwellKafkaProducer(MaxwellConfig config, Properties kafkaProperties) {
		super(config);

		if ( !kafkaProperties.containsKey("compression.type") ) {
			kafkaProperties.setProperty("compression.type", "gzip"); // enable gzip compression by default
		}

		this.kafka = new KafkaProducer<>(kafkaProperties, new ByteArraySerializer(), new ByteArraySerializer());
	}

	private String kafkaTopic(MaxwellAbstractRowsEvent e) {
		return e.getTable().getDatabase().getName();
	}

	@Override
	public void push(MaxwellAbstractRowsEvent e) throws Exception {
		ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(kafkaTopic(e), e.toJSON().getBytes());

		kafka.send(record, new KafkaCallback(e, this.config));
	}

}
