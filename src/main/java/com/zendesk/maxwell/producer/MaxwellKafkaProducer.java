package com.zendesk.maxwell.producer;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Properties;

import com.zendesk.maxwell.MaxwellAbstractRowsEvent;
import com.zendesk.maxwell.MaxwellContext;

import com.zendesk.maxwell.RowMap;
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
		this.rowMap= r;
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
	private final int numPartitions;
	private final AbstractPartitioner partitioner;

	public MaxwellKafkaProducer(MaxwellContext context, Properties kafkaProperties, String kafkaTopic) {
		super(context);

		this.topic = kafkaTopic;
		if ( this.topic == null ) {
			this.topic = "maxwell";
		}

		this.setDefaults(kafkaProperties);
		this.kafka = new KafkaProducer<>(kafkaProperties, new StringSerializer(), new StringSerializer());
		this.numPartitions = kafka.partitionsFor(topic).size(); //returns 1 for new topics
		String partitionParams = this.context.getConfig().kafkaPartitionHash + "|" + this.context.getConfig().kafkaPartitionKey;
		switch (partitionParams) {
			case "murmur3|database": this.partitioner = new Murmur3KafkaDatabasePartitioner(this.context.getConfig().murmur3Seed);
				break;
			case "murmur3|table": this.partitioner = new Murmur3KafkaTablePartitioner(this.context.getConfig().murmur3Seed);
				break;
			case "murmur3|primary_key": this.partitioner = new Murmur3KafkaPrimaryKeyPartitioner(this.context.getConfig().murmur3Seed);
				break;
			case "default|primary_key": this.partitioner = new MaxwellKafkaPrimaryKeyPartitioner();
				break;
			case "default|table": this.partitioner = new MaxwellKafkaTablePartitioner();
				break;
			case "default|database":
			default:
				this.partitioner = new MaxwellKafkaDatabasePartitioner();
				break;
		}
	}

	@Override
	public void push(RowMap r) throws Exception {
		String key = r.pkToJson();
		String value = r.toJSON();
		ProducerRecord<String, String> record =
				new ProducerRecord<>(topic, this.partitioner.kafkaPartition(r, this.numPartitions), r.pkToJson(), r.toJSON());

		kafka.send(record, new KafkaCallback(r, this.context, key, value));
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
