package com.zendesk.maxwell.producer;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.producer.partitioners.MaxwellPartitioner;
import com.zendesk.maxwell.replication.BinlogPosition;
import com.zendesk.maxwell.row.RowMap;

import com.amazonaws.services.kinesis.producer.Attempt;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.amazonaws.services.kinesis.producer.UserRecordFailedException;
import com.amazonaws.services.kinesis.producer.UserRecordResult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class KinesisCallback implements FutureCallback<UserRecordResult> {
	public static final Logger logger = LoggerFactory.getLogger(KinesisCallback.class);

	private final AbstractAsyncProducer.CallbackCompleter cc;
	private final BinlogPosition position;
	private final String json;
	private final String key;

	public KinesisCallback(AbstractAsyncProducer.CallbackCompleter cc, BinlogPosition position, String key, String json) {
		this.cc = cc;
		this.position = position;
		this.key = key;
		this.json = json;
	}

	@Override
	public void onFailure(Throwable t) {
		logger.error(t.getClass().getSimpleName() + " @ " + position + " -- " + key);
		logger.error(t.getLocalizedMessage());

		if(t instanceof UserRecordFailedException) {
			Attempt last = Iterables.getLast(((UserRecordFailedException) t).getResult().getAttempts());
			logger.error(String.format("Record failed to put - %s : %s", last.getErrorCode(), last.getErrorMessage()));
		}

		logger.error("Exception during put", t);

		cc.markCompleted();
	};

	@Override
	public void onSuccess(UserRecordResult result) {
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
	private static final Logger logger = LoggerFactory.getLogger(MaxwellKinesisProducer.class);

	private final MaxwellPartitioner partitioner;
	private final KinesisProducer kinesisProducer;
	private final String kinesisStream;

	public MaxwellKinesisProducer(MaxwellContext context, String kinesisStream) {
		super(context);

		String partitionKey = context.getConfig().producerPartitionKey;
		String partitionColumns = context.getConfig().producerPartitionColumns;
		String partitionFallback = context.getConfig().producerPartitionFallback;
		this.partitioner = new MaxwellPartitioner(partitionKey, partitionColumns, partitionFallback);
		this.kinesisStream = kinesisStream;

		Path path = Paths.get("kinesis-producer-library.properties");
		if(Files.exists(path) && Files.isRegularFile(path)) {
			KinesisProducerConfiguration config = KinesisProducerConfiguration.fromPropertiesFile(path.toString());
			this.kinesisProducer = new KinesisProducer(config);
		} else {
			this.kinesisProducer = new KinesisProducer();
		}
	}

	@Override
	public void push(RowMap r, AbstractAsyncProducer.CallbackCompleter cc) throws Exception {
		String key = this.partitioner.getHashString(r);
		String value = r.toJSON(outputConfig);

		ByteBuffer encodedValue = ByteBuffer.wrap(value.getBytes("UTF-8"));
		ListenableFuture<UserRecordResult> future = kinesisProducer.addUserRecord(kinesisStream, key, encodedValue);

		// release the reference to ease memory pressure
		if(!KinesisCallback.logger.isDebugEnabled()) {
			value = null;
		}

		FutureCallback<UserRecordResult> callback = new KinesisCallback(cc, r.getPosition(), key, value);

		Futures.addCallback(future, callback);
	}
}
