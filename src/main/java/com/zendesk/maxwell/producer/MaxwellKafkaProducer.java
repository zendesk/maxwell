package com.zendesk.maxwell.producer;

import com.zendesk.maxwell.replication.BinlogPosition;
import com.zendesk.maxwell.schema.ddl.DDLMap;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.row.RowMap.KeyFormat;
import com.zendesk.maxwell.producer.partitioners.MaxwellKafkaPartitioner;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Properties;

class KafkaCallback implements Callback {
	public static final Logger LOGGER = LoggerFactory.getLogger(MaxwellKafkaProducer.class);
	private InflightMessageList inflightMessages;
	private final MaxwellContext context;
	private final BinlogPosition position;
	private final boolean isTXCommit;
	private final String json;
	private final String key;

	public KafkaCallback(InflightMessageList inflightMessages, BinlogPosition position, boolean isTXCommit, MaxwellContext c, String key, String json) {
		this.inflightMessages = inflightMessages;
		this.context = c;
		this.position = position;
		this.isTXCommit = isTXCommit;
		this.key = key;
		this.json = json;
	}

	@Override
	public void onCompletion(RecordMetadata md, Exception e) {
		if ( e != null ) {
			LOGGER.error(e.getClass().getSimpleName() + " @ " + position + " -- " + key);
			LOGGER.error(e.getLocalizedMessage());
			if ( e instanceof RecordTooLargeException ) {
				LOGGER.error("Considering raising max.request.size broker-side.");
			}
		} else {
			if ( LOGGER.isDebugEnabled()) {
				LOGGER.debug("->  key:" + key + ", partition:" +md.partition() + ", offset:" + md.offset());
				LOGGER.debug("   " + this.json);
				LOGGER.debug("   " + position);
				LOGGER.debug("");
			}
		}
		markCompleted();
	}

	private void markCompleted() {
		if ( isTXCommit ) {
			BinlogPosition newPosition = inflightMessages.completeMessage(position);

			if ( newPosition != null ) {
				context.setPosition(newPosition);
			}
		}
	}

}

public class MaxwellKafkaProducer extends AbstractProducer {
	static final Logger LOGGER = LoggerFactory.getLogger(MaxwellKafkaProducer.class);

	private final InflightMessageList inflightMessages;
	private final KafkaProducer<String, String> kafka;
	private String topic;
	private final String ddlTopic;
	private final MaxwellKafkaPartitioner partitioner;
	private final MaxwellKafkaPartitioner ddlPartitioner;
	private final KeyFormat keyFormat;

	public MaxwellKafkaProducer(MaxwellContext context, Properties kafkaProperties, String kafkaTopic) {
		super(context);

		this.topic = kafkaTopic;
		if ( this.topic == null ) {
			this.topic = "maxwell";
		}

		this.kafka = new KafkaProducer<>(kafkaProperties, new StringSerializer(), new StringSerializer());

		String hash = context.getConfig().kafkaPartitionHash;
		String partitionKey = context.getConfig().kafkaPartitionKey;
		String partitionColumns = context.getConfig().kafkaPartitionColumns;
		String partitionFallback = context.getConfig().kafkaPartitionFallback;
		this.partitioner = new MaxwellKafkaPartitioner(hash, partitionKey, partitionColumns, partitionFallback);
		this.ddlPartitioner = new MaxwellKafkaPartitioner(hash, "database", null,"database");
		this.ddlTopic =  context.getConfig().ddlKafkaTopic;

		if ( context.getConfig().kafkaKeyFormat.equals("hash") )
			keyFormat = KeyFormat.HASH;
		else
			keyFormat = KeyFormat.ARRAY;

		this.inflightMessages = new InflightMessageList();
	}

	private Integer getNumPartitions(String topic) {
		try {
			return this.kafka.partitionsFor(topic).size(); //returns 1 for new topics
		} catch (KafkaException e) {
			LOGGER.error("Topic '" + topic + "' name does not exist. Exception: " + e.getLocalizedMessage());
			throw e;
		}
	}

	private String generateTopic(String topic, RowMap r){
		return topic.replaceAll("%\\{database\\}", r.getDatabase()).replaceAll("%\\{table\\}", r.getTable());
	}

	@Override
	public void push(RowMap r) throws Exception {
		String key = r.pkToJson(keyFormat);
		String value = r.toJSON(outputConfig);

		if ( value == null ) { // heartbeat row or other row with suppressed output
			inflightMessages.addMessage(r.getPosition());
			BinlogPosition newPosition = inflightMessages.completeMessage(r.getPosition());

			if ( newPosition != null ) {
				context.setPosition(newPosition);
			}

			return;
		}

		ProducerRecord<String, String> record;
		if (r instanceof DDLMap) {
			record = new ProducerRecord<>(this.ddlTopic, this.ddlPartitioner.kafkaPartition(r, getNumPartitions(this.ddlTopic)), key, value);
		} else {
			String topic = generateTopic(this.topic, r);
			record = new ProducerRecord<>(topic, this.partitioner.kafkaPartition(r, getNumPartitions(topic)), key, value);
		}

		if ( r.isTXCommit() )
			inflightMessages.addMessage(r.getPosition());


		/* if debug logging isn't enabled, release the reference to `value`, which can ease memory pressure somewhat */
		if ( !KafkaCallback.LOGGER.isDebugEnabled() )
			value = null;

		KafkaCallback callback = new KafkaCallback(inflightMessages, r.getPosition(), r.isTXCommit(), this.context, key, value);

		kafka.send(record, callback);
	}

	@Override
	public void writePosition(BinlogPosition p) throws SQLException {
		// ensure that we don't prematurely advance the binlog pointer.
		inflightMessages.addMessage(p);
		inflightMessages.completeMessage(p);
	}
}
