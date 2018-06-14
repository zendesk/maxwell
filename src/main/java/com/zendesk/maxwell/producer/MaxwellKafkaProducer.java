package com.zendesk.maxwell.producer;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.producer.partitioners.MaxwellKafkaPartitioner;
import com.zendesk.maxwell.replication.Position;
import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.row.RowMap.KeyFormat;
import com.zendesk.maxwell.schema.ddl.DDLMap;
import com.zendesk.maxwell.util.StoppableTask;
import com.zendesk.maxwell.util.StoppableTaskState;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeoutException;

class KafkaCallback implements Callback {
	public static final Logger LOGGER = LoggerFactory.getLogger(MaxwellKafkaProducer.class);
	private final AbstractAsyncProducer.CallbackCompleter cc;
	private final Position position;
	private final String json;
	private final String key;
	private final MaxwellContext context;

	private Counter succeededMessageCount;
	private Counter failedMessageCount;
	private Meter succeededMessageMeter;
	private Meter failedMessageMeter;

	public KafkaCallback(AbstractAsyncProducer.CallbackCompleter cc, Position position, String key, String json,
	                     Counter producedMessageCount, Counter failedMessageCount, Meter producedMessageMeter,
	                     Meter failedMessageMeter, MaxwellContext context) {
		this.cc = cc;
		this.position = position;
		this.key = key;
		this.json = json;
		this.succeededMessageCount = producedMessageCount;
		this.failedMessageCount = failedMessageCount;
		this.succeededMessageMeter = producedMessageMeter;
		this.failedMessageMeter = failedMessageMeter;
		this.context = context;
	}

	@Override
	public void onCompletion(RecordMetadata md, Exception e) {
		if ( e != null ) {
			this.failedMessageCount.inc();
			this.failedMessageMeter.mark();

			LOGGER.error(e.getClass().getSimpleName() + " @ " + position + " -- " + key);
			LOGGER.error(e.getLocalizedMessage());
			if ( e instanceof RecordTooLargeException ) {
				LOGGER.error("Considering raising max.request.size broker-side.");
			} else if (!this.context.getConfig().ignoreProducerError) {
				this.context.terminate(e);
				return;
			}
		} else {
			this.succeededMessageCount.inc();
			this.succeededMessageMeter.mark();

			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("->  key:" + key + ", partition:" + md.partition() + ", offset:" + md.offset());
				LOGGER.debug("   " + this.json);
				LOGGER.debug("   " + position);
				LOGGER.debug("");
			}
		}

		cc.markCompleted();
	}
}

public class MaxwellKafkaProducer extends AbstractProducer {
	private final ArrayBlockingQueue<RowMap> queue;
	private final MaxwellKafkaProducerWorker worker;

	public MaxwellKafkaProducer(MaxwellContext context, Properties kafkaProperties, String kafkaTopic) {
		super(context);
		this.queue = new ArrayBlockingQueue<>(100);
		this.worker = new MaxwellKafkaProducerWorker(context, kafkaProperties, kafkaTopic, this.queue);
		Thread thread = new Thread(this.worker, "maxwell-kafka-worker");
		thread.setDaemon(true);
		thread.start();
	}

	@Override
	public void push(RowMap r) throws Exception {
		this.queue.put(r);
	}

	@Override
	public StoppableTask getStoppableTask() {
		return this.worker;
	}

	@Override
	public KafkaProducerDiagnostic getDiagnostic() {
		return new KafkaProducerDiagnostic(worker, context.getConfig(), context.getPositionStoreThread());
	}
}

class MaxwellKafkaProducerWorker extends AbstractAsyncProducer implements Runnable, StoppableTask {
	static final Logger LOGGER = LoggerFactory.getLogger(MaxwellKafkaProducer.class);

	private final KafkaProducer<String, String> kafka;
	private String topic;
	private final String ddlTopic;
	private final MaxwellKafkaPartitioner partitioner;
	private final MaxwellKafkaPartitioner ddlPartitioner;
	private final KeyFormat keyFormat;
	private final boolean interpolateTopic;
	private final ArrayBlockingQueue<RowMap> queue;
	private Thread thread;
	private StoppableTaskState taskState;

	public static MaxwellKafkaPartitioner makeDDLPartitioner(String partitionHashFunc, String partitionKey) {
		if ( partitionKey.equals("table") ) {
			return new MaxwellKafkaPartitioner(partitionHashFunc, "table", null, "database");
		} else {
			return new MaxwellKafkaPartitioner(partitionHashFunc, "database", null, null);
		}
	}

	public MaxwellKafkaProducerWorker(MaxwellContext context, Properties kafkaProperties, String kafkaTopic, ArrayBlockingQueue<RowMap> queue) {
		super(context);

		this.topic = kafkaTopic;
		if ( this.topic == null ) {
			this.topic = "maxwell";
		}

		this.interpolateTopic = this.topic.contains("%{");
		this.kafka = new KafkaProducer<>(kafkaProperties, new StringSerializer(), new StringSerializer());

		String hash = context.getConfig().kafkaPartitionHash;
		String partitionKey = context.getConfig().producerPartitionKey;
		String partitionColumns = context.getConfig().producerPartitionColumns;
		String partitionFallback = context.getConfig().producerPartitionFallback;
		this.partitioner = new MaxwellKafkaPartitioner(hash, partitionKey, partitionColumns, partitionFallback);

		this.ddlPartitioner = makeDDLPartitioner(hash, partitionKey);
		this.ddlTopic =  context.getConfig().ddlKafkaTopic;

		if ( context.getConfig().kafkaKeyFormat.equals("hash") )
			keyFormat = KeyFormat.HASH;
		else
			keyFormat = KeyFormat.ARRAY;

		this.queue = queue;
		this.taskState = new StoppableTaskState("MaxwellKafkaProducerWorker");
	}

	@Override
	public void run() {
		this.thread = Thread.currentThread();
		while ( true ) {
			try {
				RowMap row = queue.take();
				if (!taskState.isRunning()) {
					taskState.stopped();
					return;
				}
				this.push(row);
			} catch ( Exception e ) {
				taskState.stopped();
				context.terminate(e);
				return;
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
		ProducerRecord<String, String> record = makeProducerRecord(r);

		/* if debug logging isn't enabled, release the reference to `value`, which can ease memory pressure somewhat */
		String value = KafkaCallback.LOGGER.isDebugEnabled() ? record.value() : null;

		KafkaCallback callback = new KafkaCallback(cc, r.getNextPosition(), record.key(), value,
				this.succeededMessageCount, this.failedMessageCount, this.succeededMessageMeter, this.failedMessageMeter, this.context);

		sendAsync(record, callback);
	}

	void sendAsync(ProducerRecord<String, String> record, Callback callback) throws Exception {
		kafka.send(record, callback);
	}

	ProducerRecord<String, String> makeProducerRecord(final RowMap r) throws Exception {
		String key = r.pkToJson(keyFormat);
		String value = r.toJSON(outputConfig);
		ProducerRecord<String, String> record;
		if (r instanceof DDLMap) {
			record = new ProducerRecord<>(this.ddlTopic, this.ddlPartitioner.kafkaPartition(r, getNumPartitions(this.ddlTopic)), key, value);
		} else {
			String topic;

			// javascript topic override
			topic = r.getKafkaTopic();
			if ( topic == null )
				topic = generateTopic(this.topic, r);

			record = new ProducerRecord<>(topic, this.partitioner.kafkaPartition(r, getNumPartitions(topic)), key, value);
		}
		return record;
	}

	@Override
	public void requestStop() {
		taskState.requestStop();
		// TODO: set a timeout once we drop support for kafka 0.8
		kafka.close();
	}

	@Override
	public void awaitStop(Long timeout) throws TimeoutException {
		taskState.awaitStop(thread, timeout);
	}

	@Override
	public StoppableTask getStoppableTask() {
		return this;
	}
}
