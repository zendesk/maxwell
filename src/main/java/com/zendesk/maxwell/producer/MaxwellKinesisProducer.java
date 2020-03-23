package com.zendesk.maxwell.producer;

import com.amazonaws.services.kinesis.producer.*;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.producer.partitioners.MaxwellKinesisPartitioner;
import com.zendesk.maxwell.replication.Position;
import com.zendesk.maxwell.row.RowMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;

class KinesisCallback implements FutureCallback<UserRecordResult> {
	public static final Logger logger = LoggerFactory.getLogger(KinesisCallback.class);

	private final AbstractAsyncProducer.CallbackCompleter cc;
	private final Position position;
	private final String json;
	private final MaxwellContext context;
	private final String key;
	private final Counter succeededMessageCount;
	private final Counter failedMessageCount;
	private final Meter succeededMessageMeter;
	private final Meter failedMessageMeter;

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
	private final ObjectMapper jsonParser;
	private final Cache<String, Set<String>> cache;

	public MaxwellKinesisProducer(MaxwellContext context, String kinesisStream) {
		super(context);

		String partitionKey = context.getConfig().producerPartitionKey;
		String partitionColumns = context.getConfig().producerPartitionColumns;
		String partitionFallback = context.getConfig().producerPartitionFallback;
		boolean kinesisMd5Keys = context.getConfig().kinesisMd5Keys;
		this.partitioner = new MaxwellKinesisPartitioner(partitionKey, partitionColumns, partitionFallback, kinesisMd5Keys);
		this.kinesisStream = kinesisStream;
		this.kinesisLinebreak = context.getConfig().kinesisLinebreak;
		this.jsonParser = new ObjectMapper();


		this.cache = CacheBuilder.newBuilder()
				.maximumSize(500)
				.expireAfterWrite(1, TimeUnit.HOURS)
				.build();

		Path path = Paths.get("kinesis-producer-library.properties");
		if (Files.exists(path) && Files.isRegularFile(path)) {
			KinesisProducerConfiguration config = KinesisProducerConfiguration.fromPropertiesFile(path.toString());
			this.kinesisProducer = new KinesisProducer(config);
		} else {
			this.kinesisProducer = new KinesisProducer();
		}
	}


	@SuppressWarnings("unchecked")
	@Override
	public void sendAsync(RowMap r, AbstractAsyncProducer.CallbackCompleter cc) throws Exception {
		String key = this.partitioner.getKinesisKey(r);
		String value = r.toJSON(outputConfig);
		if (this.kinesisLinebreak) {
			value = value + '\n';
		}
		int vsize = value.length();
		final long maxValueSize = context.getConfig().producerMaxValueSize;

		final String db = r.getDatabase();
		final String table = r.getTable();

		// extract primary key/s from RowMap
		Function<String, Set<String>> pkFromJson = (json) -> {
			try {
				String jsonData = r.pkToJson(RowMap.KeyFormat.HASH);
				Map<String, Object> map = jsonParser.readValue(jsonData, Map.class);
				return map.keySet().stream().filter(k -> k.startsWith("pk")).map(k -> {
					String[] parts = k.split("\\.");
					return parts[1];
				}).collect(Collectors.toSet());
			} catch (IOException e) {
				logger.error("error identifying table {}.{} primary keys", db, table, e);
				return Sets.newHashSet("id");
			}
		};

		final Set<String> keys = cache.get(table, () -> pkFromJson.apply(r.pkToJson(RowMap.KeyFormat.ARRAY)));
		LinkedHashMap<String, Object> reserve = new LinkedHashMap<>();

		Function<LinkedHashMap<String, Object>, String> largestValueFn = (dataHolder) -> {
			try {
				String val = r.toJSON(outputConfig);
				Optional<Map.Entry<String, Object>> maxEntry = dataHolder.entrySet().stream().max(Comparator.comparing(e -> String.valueOf(e.getValue()).length()));
				if (maxEntry.isPresent()) {
					Map.Entry<String, Object> entry = maxEntry.get();
					final String entryKey = entry.getKey();
					// if one of the table's primary key columns is to be removed, reserve it
					if (keys.contains(entryKey)) {
						// save the id column
						reserve.put(entryKey, entry.getValue());
					}
					dataHolder.remove(entryKey);
					val = r.toJSON(outputConfig);
					if (kinesisLinebreak) {
						val = val + '\n';
					}
				}
				return val;
			} catch (Exception e) {
				logger.error("error extracting the largest value", e);
				return null;
			}
		};

		// get rid of the largest value recursively to accommodate the max_data_length
		// first remove oldData, then newData
		if (vsize > maxValueSize) {
			logger.warn("{}.{} with key {} has size of {}. Removing the largest values", db, table, key, vsize);
			LinkedHashMap<String, Object> data = r.getData();
			LinkedHashMap<String, Object> oldData = r.getOldData();
			while (vsize > maxValueSize) {
				String tmpValue = largestValueFn.apply(oldData);
				if (tmpValue != null) {
					value = r.toJSON(outputConfig);
					vsize = value.length();
					if (vsize > maxValueSize) {
						tmpValue = largestValueFn.apply(data);
						if (tmpValue != null) {
							value = r.toJSON(outputConfig);
						}
					}
					if (this.kinesisLinebreak) {
						value = value + '\n';
					}
					vsize = value.length();
				} else
					break;
			}
			logger.info("{}.{} with key {} reduced to size {}", db, table, key, vsize);
			if (!reserve.isEmpty()) {
				data.putAll(reserve);
				value = r.toJSON(outputConfig);
				if (this.kinesisLinebreak) {
					value = value + '\n';
				}
			}
		}

		ByteBuffer encodedValue = ByteBuffer.wrap(value.getBytes(StandardCharsets.UTF_8));

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
			logger.error("Database:" + db + ", Table:" + table + ", PK:" + r.getRowIdentity().toConcatString() + ", Size:" + vsize);
		}
	}

	public void close() {
		this.kinesisProducer.destroy();
	}
}
