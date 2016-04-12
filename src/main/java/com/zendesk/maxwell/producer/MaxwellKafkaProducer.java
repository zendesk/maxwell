package com.zendesk.maxwell.producer;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Properties;

import com.zendesk.maxwell.MaxwellAbstractRowsEvent;
import com.zendesk.maxwell.MaxwellContext;

import com.zendesk.maxwell.RowMap;
import com.zendesk.maxwell.RowMap.KeyFormat;
import com.zendesk.maxwell.producer.partitioners.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class KafkaCallback implements Callback {
	static final Logger LOGGER = LoggerFactory.getLogger(MaxwellKafkaProducer.class);
	private final MaxwellContext context;
	private final RowMap rowMap;
	private final String json;
	private final String key;

	public KafkaCallback(RowMap r, MaxwellContext c, String key, String json) {
		this.context = c;
		this.rowMap = r;
		this.key = key;
		this.json = json;
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
					LOGGER.debug("   " + rowMap.getPosition());
					LOGGER.debug("");
				}
				if ( rowMap.isTXCommit() ) {
					context.setPosition(rowMap.getPosition());
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
	private final KafkaProducer<String, String> kafka;
	private String topic;
	private final String originalTopic;
	private final int numPartitions;
	private final MaxwellKafkaPartitioner partitioner;
	private final KeyFormat keyFormat;

	public MaxwellKafkaProducer(MaxwellContext context, Properties kafkaProperties, String kafkaTopic) {
		super(context);

		this.topic = kafkaTopic;
		
		if ( this.topic == null ) {
			this.topic = "maxwell";
		}
		
		this.originalTopic = this.topic;

		this.setDefaults(kafkaProperties);
		this.kafka = new KafkaProducer<>(kafkaProperties, new StringSerializer(), new StringSerializer());
		this.numPartitions = kafka.partitionsFor(topic).size(); //returns 1 for new topics

		String hash = context.getConfig().kafkaPartitionHash;
		String partitionKey = context.getConfig().kafkaPartitionKey;
		this.partitioner = new MaxwellKafkaPartitioner(hash, partitionKey);

		if ( context.getConfig().kafkaKeyFormat.equals("hash") )
			keyFormat = KeyFormat.HASH;
		else
			keyFormat = KeyFormat.ARRAY;
	}

	@Override
	public void push(RowMap r) throws Exception {
		String key = r.pkToJson(keyFormat);
		String value = r.toJSON();
		addTableTopic(context.getConfig().useTableTopic, r);

		ProducerRecord<String, String> record =
				new ProducerRecord<>(topic, this.partitioner.kafkaPartition(r, this.numPartitions), key, value);

		kafka.send(record, new KafkaCallback(r, this.context, key, value));
	}

	private void addTableTopic(boolean useTableTopic, RowMap r) {
		if ( useTableTopic == true ) {
			this.topic = this.originalTopic + "-" + r.getTable();
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
