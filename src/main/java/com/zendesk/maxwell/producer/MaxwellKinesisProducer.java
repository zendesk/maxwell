package com.zendesk.maxwell.producer;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.producer.partitioners.MaxwellKinesisPartitioner;
import com.zendesk.maxwell.replication.BinlogPosition;
import com.zendesk.maxwell.row.RowMap;

import com.amazonaws.services.kinesis.producer.Attempt;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.amazonaws.services.kinesis.producer.UserRecordFailedException;
import com.amazonaws.services.kinesis.producer.UserRecordResult;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class KinesisCallback implements FutureCallback<UserRecordResult> {
	public static final Logger logger = LoggerFactory.getLogger(KinesisCallback.class);

	private final AbstractAsyncProducer.CallbackCompleter cc;
	private final BinlogPosition position;
	private final String json;
	private final String key;

	private AtomicInteger successRecords;
	private AtomicInteger errorRecords;
	private final KinesisProducer kinesisProducer;
	private final String kinesisStream;
	private int attempts = 0;
	private final int maxAttempts;
	private AtomicLong lastInfoLog;

	private static final Random RANDOM = new Random();

	public KinesisCallback(AbstractAsyncProducer.CallbackCompleter cc, BinlogPosition position, String key, String json, AtomicInteger successRecords, AtomicInteger errorRecords, KinesisProducer kinesisProducer, String kinesisStream, int attempts, int maxAttempts, AtomicLong lastInfoLog) {
		this.cc = cc;
		this.position = position;
		this.key = key;
		this.json = json;
		this.successRecords = successRecords;
		this.errorRecords = errorRecords;
		this.kinesisProducer = kinesisProducer;
		this.kinesisStream = kinesisStream;
		this.attempts = attempts;
		this.maxAttempts = maxAttempts;
		this.lastInfoLog = lastInfoLog;
	}

	@Override
	public void onFailure(Throwable t) {
		logger.error(t.getClass().getSimpleName() + " @ " + position + " -- " + key);
		logger.error(t.getLocalizedMessage());

		if(attempts>=maxAttempts) {
			int errRecords = errorRecords.incrementAndGet();

			if(t instanceof UserRecordFailedException) {
				Attempt last = Iterables.getLast(((UserRecordFailedException) t).getResult().getAttempts());
				UserRecordResult result = ((UserRecordFailedException) t).getResult();
				List<String> errorList = new ArrayList<String>();
				for(Attempt attempt : result.getAttempts()){
				   errorList.add(String.format(
						   "Delay after prev attempt: %d ms, "
								   + "Duration: %d ms, Code: %s, "
								   + "Message: %s",
						   attempt.getDelay(), attempt.getDuration(),
						   attempt.getErrorCode(),
						   attempt.getErrorMessage()));
				}
				String errorListStr = StringUtils.join(errorList, "n");
				logger.error(String.format(
						"Record failed to put, attempts: %d. Errors: %s", result.getAttempts().size(), errorListStr));
				logger.error(String.format("Number of failed records up to now: %d", errRecords));
				logger.error(String.format("Errored message: %s", json));
			}

			cc.markCompleted();
		} else {
			attempts++;

			logger.warn(String.format("Record failed to put. Retrying. Attempt: %d ", attempts));

			try {
				ByteBuffer encodedValue = ByteBuffer.wrap(json.getBytes("UTF-8"));
				ListenableFuture<UserRecordResult> future = kinesisProducer.addUserRecord(kinesisStream, key, encodedValue);

				FutureCallback<UserRecordResult> callback = new KinesisCallback(cc, position, key, json, successRecords, errorRecords, kinesisProducer, kinesisStream, attempts, maxAttempts, lastInfoLog);

				Futures.addCallback(future, callback);
			} catch (UnsupportedEncodingException e) {
				logger.error("Error encoding message. Message: " + json + ". Error: " + e.getMessage());
			}
		}
	};

	@Override
	public void onSuccess(UserRecordResult result) {
		int succRecords = successRecords.incrementAndGet();
		if(logger.isInfoEnabled()) {
			// Only log with a small probability, otherwise it'll be very spammy
			// Or when it is more than 30 seconds since last log
			if (RANDOM.nextDouble() < 1e-5 || lastInfoLog.getAndSet(new Date().getTime()) < new Date().getTime() - 30 * 1000) {
				long totalTime = 0L;
				for (Attempt attempt : result.getAttempts()) {
					totalTime += attempt.getDelay() + attempt.getDuration();
				}
				logger.info(String.format(
						"Succesfully put record, sequenceNumber=%s, "
								+ "shardId=%s, took %d attempts, "
								+ "totalling %s ms",
						result.getSequenceNumber(),
						result.getShardId(), result.getAttempts().size(),
						totalTime));
				logger.debug(String.format("Number of success records up to now: %d", succRecords));
			}
		}
		cc.markCompleted();
	};
}

public class MaxwellKinesisProducer extends AbstractProducer {
	private final ArrayBlockingQueue<RowMap> queue;
	private final MaxwellKinesisProducerWorker worker;

	public MaxwellKinesisProducer(MaxwellContext context, String kinesisStream) {
		super(context);
		this.queue = new ArrayBlockingQueue<>(100);
		this.worker = new MaxwellKinesisProducerWorker(context, kinesisStream, this.queue);
		new Thread(this.worker, "maxwell-kinesis-worker").start();
	}

	@Override
	public void push(RowMap r) throws Exception {
		this.queue.put(r);
	}

}

class MaxwellKinesisProducerWorker extends AbstractAsyncProducerWorker {

	private static final Logger logger = LoggerFactory.getLogger(MaxwellKinesisProducer.class);

	private final MaxwellKinesisPartitioner partitioner;
	private final KinesisProducer kinesisProducer;
	private final String kinesisStream;
	private final int maxAttempts;
	private final int maxBufferedRecords;
	private AtomicInteger successRecords = new AtomicInteger(0);
	private AtomicInteger errorRecords = new AtomicInteger(0);
	private AtomicLong lastInfoLog = new AtomicLong(0L);
	private final ArrayBlockingQueue<RowMap> queue;

	public MaxwellKinesisProducerWorker(MaxwellContext context, String kinesisStream, ArrayBlockingQueue<RowMap> queue) {
		super(context, queue);
		String partitionKey = context.getConfig().producerPartitionKey;
		String partitionColumns = context.getConfig().producerPartitionColumns;
		String partitionFallback = context.getConfig().producerPartitionFallback;
		boolean kinesisMd5Keys = context.getConfig().kinesisMd5Keys;
		this.partitioner = new MaxwellKinesisPartitioner(partitionKey, partitionColumns, partitionFallback, kinesisMd5Keys);
		this.kinesisStream = kinesisStream;

		this.maxAttempts = context.getConfig().kinesisMaxAttempts;
		this.maxBufferedRecords = context.getConfig().kinesisMaxBufferedRecords;

		Path path = Paths.get("kinesis-producer-library.properties");
		if(Files.exists(path) && Files.isRegularFile(path)) {
			KinesisProducerConfiguration config = KinesisProducerConfiguration.fromPropertiesFile(path.toString());
			this.kinesisProducer = new KinesisProducer(config);
		} else {
			this.kinesisProducer = new KinesisProducer();
		}
		this.queue = queue;
	}

	@Override
	public void sendAsync(RowMap r, AbstractAsyncProducer.CallbackCompleter cc) throws Exception {
		String key = this.partitioner.getKinesisKey(r);
		String value = r.toJSON(outputConfig);

		// Wait for KPL to finish sending events
		while(kinesisProducer.getOutstandingRecordsCount()> this.maxBufferedRecords) {
			Thread.sleep(1);
		}

		ByteBuffer encodedValue = ByteBuffer.wrap(value.getBytes("UTF-8"));
		ListenableFuture<UserRecordResult> future = kinesisProducer.addUserRecord(kinesisStream, key, encodedValue);

		FutureCallback<UserRecordResult> callback = new KinesisCallback(cc, r.getPosition(), key, value, successRecords, errorRecords, kinesisProducer, kinesisStream, 0, maxAttempts, lastInfoLog);

		Futures.addCallback(future, callback);
	}

	@Override
	public void release() {
		kinesisProducer.flushSync();
		kinesisProducer.destroy();
	}
}
