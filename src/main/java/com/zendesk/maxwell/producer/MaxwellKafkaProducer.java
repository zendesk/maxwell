package com.zendesk.maxwell.producer;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.zendesk.maxwell.MaxwellMetrics;
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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;


class KafkaCallback implements Callback {
	public static final Logger LOGGER = LoggerFactory.getLogger(MaxwellKafkaProducer.class);
	private final AbstractAsyncProducer.CallbackCompleter cc;
	private final BinlogPosition position;
	private final String json;
	private final String key;
	private final Timer timer;

	public KafkaCallback(AbstractAsyncProducer.CallbackCompleter cc, BinlogPosition position, String key, String json, Timer timer) {
		this.cc = cc;
		this.position = position;
		this.key = key;
		this.json = json;
		this.timer = timer;
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
		cc.markCompleted();
		timer.update(cc.timeToSendMS(), TimeUnit.MILLISECONDS);
	}
}


public class MaxwellKafkaProducer extends AbstractProducer {
	private final ArrayBlockingQueue<RowMap> queue;
	private final MaxwellKafkaProducerWorker worker;

	public MaxwellKafkaProducer(MaxwellContext context, Properties kafkaProperties, String kafkaTopic) {
		super(context);
		this.queue = new ArrayBlockingQueue<>(100);
		this.worker = new MaxwellKafkaProducerWorker(context, kafkaProperties, kafkaTopic, this.queue);
		new Thread(this.worker, "maxwell-kafka-worker").start();

	}

	@Override
	public void push(RowMap r) throws Exception {
		this.queue.put(r);
	}
}

class MaxwellKafkaProducerWorker extends AbstractAsyncProducer implements Runnable {
	static final Logger LOGGER = LoggerFactory.getLogger(MaxwellKafkaProducer.class);

	private final KafkaProducer<String, String> kafka;
	private String topic;
	private final String ddlTopic;
	private final MaxwellKafkaPartitioner partitioner;
	private final MaxwellKafkaPartitioner ddlPartitioner;
	private final KeyFormat keyFormat;
	private final boolean interpolateTopic;
	private final Timer metricsTimer;
	private final ArrayBlockingQueue<RowMap> queue;

	public MaxwellKafkaProducerWorker(MaxwellContext context, Properties kafkaProperties, String kafkaTopic, ArrayBlockingQueue<RowMap> queue) {
		super(context);

		this.topic = kafkaTopic;
		if ( this.topic == null ) {
			this.topic = "maxwell";
		}

		this.interpolateTopic = kafkaTopic.contains("%{");
		this.kafka = new KafkaProducer<>(kafkaProperties, new StringSerializer(), new StringSerializer());

		String hash = context.getConfig().kafkaPartitionHash;
		String partitionKey = context.getConfig().producerPartitionKey;
		String partitionColumns = context.getConfig().producerPartitionColumns;
		String partitionFallback = context.getConfig().producerPartitionFallback;
		this.partitioner = new MaxwellKafkaPartitioner(hash, partitionKey, partitionColumns, partitionFallback);
		this.ddlPartitioner = new MaxwellKafkaPartitioner(hash, "database", null,"database");
		this.ddlTopic =  context.getConfig().ddlKafkaTopic;

		if ( context.getConfig().kafkaKeyFormat.equals("hash") )
			keyFormat = KeyFormat.HASH;
		else
			keyFormat = KeyFormat.ARRAY;

		this.metricsTimer = MaxwellMetrics.registry.timer(MetricRegistry.name(MaxwellKafkaProducer.class, "kafka-overall-time"));
		this.queue = queue;
	}

	@Override
	public void run() {
		while ( true ) {
			try {
				RowMap row = queue.take();
				this.push(row);
			} catch ( Exception e ) {
				throw new RuntimeException(e);
			}
		}
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
		if ( interpolateTopic )
			return topic.replaceAll("%\\{database\\}", r.getDatabase()).replaceAll("%\\{table\\}", r.getTable());
		else
			return topic;
	}

	@Override
	public void sendAsync(RowMap r, AbstractAsyncProducer.CallbackCompleter cc) throws Exception {
		String key = r.pkToJson(keyFormat);
		String value = r.toJSON(outputConfig);

		ProducerRecord<String, String> record;
		if (r instanceof DDLMap) {
			record = new ProducerRecord<>(this.ddlTopic, this.ddlPartitioner.kafkaPartition(r, getNumPartitions(this.ddlTopic)), key, value);
		} else {
			String topic = generateTopic(this.topic, r);
			record = new ProducerRecord<>(topic, this.partitioner.kafkaPartition(r, getNumPartitions(topic)), key, value);
		}

		/* if debug logging isn't enabled, release the reference to `value`, which can ease memory pressure somewhat */
		if ( !KafkaCallback.LOGGER.isDebugEnabled() )
			value = null;

		KafkaCallback callback = new KafkaCallback(cc, r.getPosition(), key, value, metricsTimer);

		kafka.send(record, callback);
	}
}
