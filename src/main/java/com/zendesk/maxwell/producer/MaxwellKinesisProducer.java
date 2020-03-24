package com.zendesk.maxwell.producer;

import com.amazonaws.services.kinesis.producer.*;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.producer.partitioners.MaxwellKinesisPartitioner;
import com.zendesk.maxwell.replication.Position;
import com.zendesk.maxwell.row.RowMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;

class KinesisCallback implements FutureCallback<UserRecordResult> {
	public static final Logger logger = LoggerFactory.getLogger(KinesisCallback.class);

	private final AbstractAsyncProducer.CallbackCompleter cc;
	private final Position position;
	private final String json;
	private MaxwellContext context;
	private final String key;
	private Counter succeededMessageCount;
	private Counter failedMessageCount;
	private Meter succeededMessageMeter;
	private Meter failedMessageMeter;

	public KinesisCallback(AbstractAsyncProducer.CallbackCompleter cc, Position position, String key, String json,
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
	public void onFailure(Throwable t) {
		logger.error(t.getClass().getSimpleName() + " @ " + position + " -- " + key);
		logger.error(t.getLocalizedMessage());
		this.failedMessageCount.inc();
		this.failedMessageMeter.mark();

		if (t instanceof UserRecordFailedException) {
			Attempt last = Iterables.getLast(((UserRecordFailedException) t).getResult().getAttempts());
			logger.error(String.format("Record failed to put - %s : %s", last.getErrorCode(), last.getErrorMessage()));
		}

		logger.error("Exception during put", t);

		if (!context.getConfig().ignoreProducerError) {
			context.terminate(new RuntimeException(t));
		} else {
			cc.markCompleted();
		}
	}

	@Override
	public void onSuccess(UserRecordResult result) {
		this.succeededMessageCount.inc();
		this.succeededMessageMeter.mark();
		if (logger.isDebugEnabled()) {
			logger.debug("->  key:" + key + ", shard id:" + result.getShardId() + ", sequence number:" + result.getSequenceNumber());
			logger.debug("   " + json);
			logger.debug("   " + position);
			logger.debug("");
		}

		cc.markCompleted();
	}
}

public class MaxwellKinesisProducer extends AbstractAsyncProducer {
	private static final Logger logger = LoggerFactory.getLogger(MaxwellKinesisProducer.class);

	private final MaxwellKinesisPartitioner partitioner;
	private final KinesisProducer kinesisProducer;
	private final String kinesisStream;
	private final boolean kinesisLinebreak;

	public MaxwellKinesisProducer(MaxwellContext context, String kinesisStream) {
		super(context);

		String partitionKey = context.getConfig().producerPartitionKey;
		String partitionColumns = context.getConfig().producerPartitionColumns;
		String partitionFallback = context.getConfig().producerPartitionFallback;
		boolean kinesisMd5Keys = context.getConfig().kinesisMd5Keys;
		this.partitioner = new MaxwellKinesisPartitioner(partitionKey, partitionColumns, partitionFallback, kinesisMd5Keys);
		this.kinesisStream = kinesisStream;
		this.kinesisLinebreak = context.getConfig().kinesisLinebreak;

		Path path = Paths.get("kinesis-producer-library.properties");
		if (Files.exists(path) && Files.isRegularFile(path)) {
			KinesisProducerConfiguration config = KinesisProducerConfiguration.fromPropertiesFile(path.toString());
			this.kinesisProducer = new KinesisProducer(config);
		} else {
			this.kinesisProducer = new KinesisProducer();
		}
	}

	@Override
	public void sendAsync(RowMap r, AbstractAsyncProducer.CallbackCompleter cc) throws Exception {
		String key = this.partitioner.getKinesisKey(r);
		String value = r.toJSON(outputConfig);
		if (this.kinesisLinebreak) {
			value = value + '\n';
		}
		int vsize = value.length();
		final long maxValueSize = context.getConfig().producerMaxValueSize;

		// get rid of largest value recursively to accommodate max_data_length
		if (vsize > maxValueSize) {
			logger.warn("{}.{} with key {} has size of {}. Removing largest values", r.getDatabase(), r.getTable(), key, vsize);
			LinkedHashMap<String, Object> data = r.getData();
			while (vsize > maxValueSize) {
				Optional<Map.Entry<String, Object>> maxEntry = data.entrySet().stream().max(Comparator.comparing(e -> e.getValue().toString().length()));
				if (maxEntry.isPresent()) {
					data.remove(maxEntry.get().getKey());
					value = r.toJSON(outputConfig);
					if (this.kinesisLinebreak) {
						value = value + '\n';
					}
					vsize = value.length();
				} else
					break;
			}
			logger.info("{}.{} with key {} reduced to size {}", r.getDatabase(), r.getTable(), key, vsize);
		}

		ByteBuffer encodedValue = ByteBuffer.wrap(value.getBytes("UTF-8"));

		// release the reference to ease memory pressure
		if (!KinesisCallback.logger.isDebugEnabled()) {
			value = null;
		}

		FutureCallback<UserRecordResult> callback = new KinesisCallback(cc, r.getNextPosition(), key, value,
				this.succeededMessageCount, this.failedMessageCount, this.succeededMessageMeter, this.failedMessageMeter, this.context);

		try {
			ListenableFuture<UserRecordResult> future = kinesisProducer.addUserRecord(kinesisStream, key, encodedValue);
			Futures.addCallback(future, callback, directExecutor());
		} catch (IllegalArgumentException t) {
			callback.onFailure(t);
			logger.error("Database:" + r.getDatabase() + ", Table:" + r.getTable() + ", PK:" + r.getRowIdentity().toConcatString() + ", Size:" + vsize);
		}
	}

	public void close() {
		this.kinesisProducer.destroy();
	}
}
