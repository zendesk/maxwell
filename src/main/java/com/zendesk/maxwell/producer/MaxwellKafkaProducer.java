package com.zendesk.maxwell.producer;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.producer.partitioners.MaxwellKafkaPartitioner;
import com.zendesk.maxwell.replication.Position;
import com.zendesk.maxwell.row.RowIdentity;
import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.row.RowMap.KeyFormat;
import com.zendesk.maxwell.schema.ddl.DDLMap;
import com.zendesk.maxwell.util.StoppableTask;
import com.zendesk.maxwell.util.StoppableTaskState;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeoutException;

class KafkaCallback implements Callback {
	public static final Logger LOGGER = LoggerFactory.getLogger(MaxwellKafkaProducer.class);
	private final AbstractAsyncProducer.CallbackCompleter cc;
	private final Position position;
	private final String json;
	private final RowIdentity key;
	private final String topic;
	private final String fallbackTopic;
	private final MaxwellKafkaProducerWorker producer;
	private final MaxwellContext context;

	private Counter succeededMessageCount;
	private Counter failedMessageCount;
	private Meter succeededMessageMeter;
	private Meter failedMessageMeter;

	public KafkaCallback(AbstractAsyncProducer.CallbackCompleter cc, Position position, RowIdentity key, String json,
	                     Counter producedMessageCount, Counter failedMessageCount, Meter producedMessageMeter,
	                     Meter failedMessageMeter, String topic, String fallbackTopic, MaxwellContext context,
	                     MaxwellKafkaProducerWorker producer) {
		this.cc = cc;
		this.position = position;
		this.key = key;
		this.json = json;
		this.succeededMessageCount = producedMessageCount;
		this.failedMessageCount = failedMessageCount;
		this.succeededMessageMeter = producedMessageMeter;
		this.failedMessageMeter = failedMessageMeter;
		this.topic = topic;
		this.fallbackTopic = fallbackTopic;
		this.producer = producer;
		this.context = context;
	}

	@Override
	public void onCompletion(RecordMetadata md, Exception e) {
		if ( e != null ) {
			this.failedMessageCount.inc();
			this.failedMessageMeter.mark();

			LOGGER.error(e.getClass().getSimpleName() + " @ " + position + " -- " + topic + ": " + key);
			LOGGER.error(e.getLocalizedMessage());

			boolean nonFatal = e instanceof RecordTooLargeException || context.getConfig().ignoreProducerError;
			if (nonFatal) {
				if (this.fallbackTopic == null) {
					cc.markCompleted();
				} else {
					publishFallback(md, e);
				}
			} else {
				context.terminate(e);
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
			cc.markCompleted();
		}
	}

	private void publishFallback(RecordMetadata md, Exception e) {
		// When publishing a fallback record, make a callback
		// with no fallback topic to avoid infinite loops
		KafkaCallback cb = new KafkaCallback(cc, position, key, json,
			succeededMessageCount, failedMessageCount, succeededMessageMeter,
			failedMessageMeter, topic, null, context, producer);
		producer.enqueueFallbackRow(fallbackTopic, key, cb, md, e);
	}

	String getFallbackTopic() {
		return fallbackTopic;
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

	private final Producer<String, String> kafka;
	private final String topic;
	private final String ddlTopic;
	private final MaxwellKafkaPartitioner partitioner;
	private final MaxwellKafkaPartitioner ddlPartitioner;
	private final KeyFormat keyFormat;
	private final boolean interpolateTopic;
	private final ArrayBlockingQueue<RowMap> queue;
	private Thread thread;
	private StoppableTaskState taskState;
	private String deadLetterTopic;
	private final ConcurrentLinkedQueue<Pair<ProducerRecord<String,String>, KafkaCallback>> deadLetterQueue;

	public static MaxwellKafkaPartitioner makeDDLPartitioner(String partitionHashFunc, String partitionKey) {
		if ( partitionKey.equals("table") ) {
			return new MaxwellKafkaPartitioner(partitionHashFunc, "table", null, "database");
		} else {
			return new MaxwellKafkaPartitioner(partitionHashFunc, "database", null, null);
		}
	}

	public MaxwellKafkaProducerWorker(MaxwellContext context, String kafkaTopic, ArrayBlockingQueue<RowMap> queue,
		Producer<String,String> producer)
	{
		super(context);

		if ( kafkaTopic == null ) {
			this.topic = "maxwell";
		} else {
			this.topic = kafkaTopic;
		}

		this.interpolateTopic = this.topic.contains("%{");
		this.kafka = producer;

		String hash = context.getConfig().kafkaPartitionHash;
		String partitionKey = context.getConfig().producerPartitionKey;
		String partitionColumns = context.getConfig().producerPartitionColumns;
		String partitionFallback = context.getConfig().producerPartitionFallback;
		this.partitioner = new MaxwellKafkaPartitioner(hash, partitionKey, partitionColumns, partitionFallback);

		this.ddlPartitioner = makeDDLPartitioner(hash, partitionKey);
		this.ddlTopic =  context.getConfig().ddlKafkaTopic;
		this.deadLetterTopic = context.getConfig().deadLetterTopic;
		this.deadLetterQueue = new ConcurrentLinkedQueue<>();

		if ( context.getConfig().kafkaKeyFormat.equals("hash") )
			keyFormat = KeyFormat.HASH;
		else
			keyFormat = KeyFormat.ARRAY;

		this.queue = queue;
		this.taskState = new StoppableTaskState("MaxwellKafkaProducerWorker");
	}

	public MaxwellKafkaProducerWorker(MaxwellContext context, Properties kafkaProperties, String kafkaTopic,
		ArrayBlockingQueue<RowMap> queue)
	{
		this(context, kafkaTopic, queue,
			new KafkaProducer<String,String>(kafkaProperties, new StringSerializer(), new StringSerializer()));
	}

	@Override
	public void run() {
		this.thread = Thread.currentThread();
		while ( true ) {
			try {
				drainDeadLetterQueue();
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

	void drainDeadLetterQueue() {
		Pair<ProducerRecord<String, String>, KafkaCallback> pair;
		while ((pair = deadLetterQueue.poll()) != null) {
			sendAsync(pair.getLeft(), pair.getRight());
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

	private String generateTopic(String topic, RowIdentity pk){
		if ( interpolateTopic )
			return topic.replaceAll("%\\{database\\}", pk.getDatabase()).replaceAll("%\\{table\\}", pk.getTable());
		else
			return topic;
	}

	@Override
	public void sendAsync(RowMap r, AbstractAsyncProducer.CallbackCompleter cc) throws Exception {
		ProducerRecord<String, String> record = makeProducerRecord(r);

		/* if debug logging isn't enabled, release the reference to `value`, which can ease memory pressure somewhat */
		String value = KafkaCallback.LOGGER.isDebugEnabled() ? record.value() : null;

		KafkaCallback callback = new KafkaCallback(cc, r.getNextPosition(), r.getRowIdentity(), value,
				this.succeededMessageCount, this.failedMessageCount, this.succeededMessageMeter, this.failedMessageMeter,
				this.topic, this.deadLetterTopic, this.context, this);

		sendAsync(record, callback);
	}

	public void enqueueFallbackRow(String topic, RowIdentity fallbackRecord, KafkaCallback callback, RecordMetadata md, Exception reason) {
		// This code may be executed from the `kafka-producer-network-thread`, which will deadlock if we try to directly call send().
		// So enqueue a message for the worker thread to pick up.
		LOGGER.info("publishing fallback record to " + topic + ": " + fallbackRecord);
		try {
			ProducerRecord<String, String> record = makeFallbackRecord(topic, fallbackRecord, reason);
			deadLetterQueue.add(Pair.of(record, callback));
		} catch (Exception fallbackEx) {
			callback.onCompletion(md, fallbackEx);
		}
	}

	void sendAsync(ProducerRecord<String, String> record, Callback callback) {
		kafka.send(record, callback);
	}

	ProducerRecord<String, String> makeProducerRecord(final RowMap r) throws Exception {
		RowIdentity pk = r.getRowIdentity();
		String key = r.pkToJson(keyFormat);
		String value = r.toJSON(outputConfig);
		ProducerRecord<String, String> record;
		if (r instanceof DDLMap) {
			record = new ProducerRecord<>(this.ddlTopic, this.ddlPartitioner.kafkaPartition(r, getNumPartitions(this.ddlTopic)), key, value);
		} else {
			String topic;

			// javascript topic override
			topic = r.getKafkaTopic();
			if ( topic == null ) {
				topic = generateTopic(this.topic, pk);
			}
			LOGGER.debug("context.getConfig().producerPartitionKey = " + context.getConfig().producerPartitionKey);

			record = new ProducerRecord<>(topic, this.partitioner.kafkaPartition(r, getNumPartitions(topic)), key, value);
		}
		return record;
	}

	ProducerRecord<String, String> makeFallbackRecord(String fallbackTopic, final RowIdentity pk, Exception reason) throws Exception {
		String key = pk.toKeyJson(keyFormat);
		String value = pk.toFallbackValueWithReason(reason.getClass().getSimpleName());
		String topic = generateTopic(fallbackTopic, pk);
		return new ProducerRecord<>(topic, key, value);
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

	// force-close for tests.
	public void close() {
		kafka.close();
	}

	@Override
	public StoppableTask getStoppableTask() {
		return this;
	}
}
