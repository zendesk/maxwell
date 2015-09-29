package com.zendesk.maxwell.producer;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Properties;

import com.zendesk.maxwell.MaxwellAbstractRowsEvent;
import com.zendesk.maxwell.MaxwellContext;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class KafkaCallback implements Callback {
	static final Logger LOGGER = LoggerFactory.getLogger(MaxwellKafkaProducer.class);
	private final MaxwellContext context;
	private final MaxwellAbstractRowsEvent event;
	private final String json;
	private final String key;
	private final boolean lastRowInEvent;

	public KafkaCallback(MaxwellAbstractRowsEvent e, MaxwellContext c, String key, String json, boolean lastRowInEvent) {
		this.context = c;
		this.event = e;
		this.key = key;
		this.json = json;
		this.lastRowInEvent = lastRowInEvent;
	}

	@Override
	public void onCompletion(RecordMetadata md, Exception e) {
		if ( e != null ) {
			e.printStackTrace();
		} else {
			try {
				if ( LOGGER.isDebugEnabled()) {
					LOGGER.debug("->  key:" + key + ", partition:" +md.partition() + ", offset:" + md.offset());
					LOGGER.debug("   " + this.json);
					LOGGER.debug("   " + event.getNextBinlogPosition());
					LOGGER.debug("");
				}
				if ( this.lastRowInEvent ) {
					context.setPosition(event);
				}
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
		}
	}
}

public class MaxwellKafkaProducer extends AbstractProducer {
	static final Object KAFKA_DEFAULTS[] = {
		"compression.type", "gzip",
		"metadata.fetch.timeout.ms", 5000
	};
	private final KafkaProducer<byte[], byte[]> kafka;
	private String topic;
	private final int numPartitions;

	public MaxwellKafkaProducer(MaxwellContext context, Properties kafkaProperties, String kafkaTopic) {
		super(context);

		this.topic = kafkaTopic;
		if ( this.topic == null ) {
			this.topic = "maxwell";
		}

		this.setDefaults(kafkaProperties);
		this.kafka = new KafkaProducer<>(kafkaProperties, new ByteArraySerializer(), new ByteArraySerializer());
		this.numPartitions = kafka.partitionsFor(topic).size(); //returns 1 for new topics
	}

	public int kafkaPartition(MaxwellAbstractRowsEvent e){
		String db = e.getDatabase().getName();
		return Math.abs(db.hashCode() % numPartitions);
	}

	@Override
	public void push(MaxwellAbstractRowsEvent e) throws Exception {
		Iterator<String> i = e.toJSONStrings().iterator();
		Iterator<String> j = e.getPKStrings().iterator();

		while ( i.hasNext() && j.hasNext() ) {
			String json = i.next();
			String key = j.next();

			ProducerRecord<byte[], byte[]> record =
					new ProducerRecord<>(topic, kafkaPartition(e), key.getBytes(), json.getBytes());

			kafka.send(record, new KafkaCallback(e, this.context, key, json, !i.hasNext()));
		}

	}

	private void setDefaults(Properties p) {
		for(int i=0 ; i < KAFKA_DEFAULTS.length; i += 2) {
			String key = (String) KAFKA_DEFAULTS[i];
			Object val = KAFKA_DEFAULTS[i + 1];

			if ( !p.containsKey(key)) {
				p.put(key, val);
			}
		}
	}
}
