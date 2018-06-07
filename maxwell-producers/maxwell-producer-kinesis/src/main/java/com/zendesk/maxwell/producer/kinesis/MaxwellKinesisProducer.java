package com.zendesk.maxwell.producer.kinesis;

import com.amazonaws.services.kinesis.producer.Attempt;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.UserRecordFailedException;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.zendesk.maxwell.api.MaxwellContext;
import com.zendesk.maxwell.api.replication.Position;
import com.zendesk.maxwell.api.row.RowMap;
import com.zendesk.maxwell.api.producer.AbstractAsyncProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

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

		if(t instanceof UserRecordFailedException) {
			Attempt last = Iterables.getLast(((UserRecordFailedException) t).getResult().getAttempts());
			logger.error(String.format("Record failed to put - %s : %s", last.getErrorCode(), last.getErrorMessage()));
		}

		logger.error("Exception during put", t);

		if (!context.getConfig().isIgnoreProducerError()) {
			context.terminate(new RuntimeException(t));
		} else {
			cc.markCompleted();
		}
	};

	@Override
	public void onSuccess(UserRecordResult result) {
		this.succeededMessageCount.inc();
		this.succeededMessageMeter.mark();
		if(logger.isDebugEnabled()) {
			logger.debug("->  key:" + key + ", shard id:" + result.getShardId() + ", sequence number:" + result.getSequenceNumber());
			logger.debug("   " + json);
			logger.debug("   " + position);
			logger.debug("");
		}

		cc.markCompleted();
	};
}

public class MaxwellKinesisProducer extends AbstractAsyncProducer {
	private final MaxwellKinesisPartitioner partitioner;
	private final KinesisProducer kinesisProducer;
	private final String kinesisStream;

	public MaxwellKinesisProducer(MaxwellContext context, KinesisProducerConfiguration kinesisProducerConfiguration) {
		super(context);

		String partitionKey = context.getConfig().getProducerPartitionKey();
		String partitionColumns = context.getConfig().getProducerPartitionColumns();
		String partitionFallback = context.getConfig().getProducerPartitionFallback();

		boolean kinesisMd5Keys = kinesisProducerConfiguration.isKinesisMd5Keys();
		this.partitioner = new MaxwellKinesisPartitioner(partitionKey, partitionColumns, partitionFallback, kinesisMd5Keys);
		this.kinesisStream = kinesisProducerConfiguration.getKinesisStream();

		Path path = Paths.get("kinesis-producer-library.properties");
		if(Files.exists(path) && Files.isRegularFile(path)) {
			com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration config = com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration.fromPropertiesFile(path.toString());
			this.kinesisProducer = new KinesisProducer(config);
		} else {
			this.kinesisProducer = new KinesisProducer();
		}
	}

	@Override
	public void sendAsync(RowMap r, AbstractAsyncProducer.CallbackCompleter cc) throws Exception {
		String key = this.partitioner.getKinesisKey(r);
		String value = r.toJSON(outputConfig);

		ByteBuffer encodedValue = ByteBuffer.wrap(value.getBytes("UTF-8"));
		ListenableFuture<UserRecordResult> future = kinesisProducer.addUserRecord(kinesisStream, key, encodedValue);

		// release the reference to ease memory pressure
		if(!KinesisCallback.logger.isDebugEnabled()) {
			value = null;
		}

		FutureCallback<UserRecordResult> callback = new KinesisCallback(cc, r.getPosition(), key, value,
				this.succeededMessageCount, this.failedMessageCount, this.succeededMessageMeter, this.failedMessageMeter, this.context);

		Futures.addCallback(future, callback);
	}
}