package com.zendesk.maxwell.replication;

import binlogdata.Binlogdata;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.zendesk.maxwell.MaxwellMysqlConfig;
import com.zendesk.maxwell.monitoring.Metrics;
import com.zendesk.maxwell.producer.AbstractProducer;
import com.zendesk.maxwell.producer.MaxwellOutputConfig;
import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.row.RowMapBuffer;
import com.zendesk.maxwell.schema.Schema;
import com.zendesk.maxwell.schema.SchemaStore;
import com.zendesk.maxwell.schema.SchemaStoreException;
import com.zendesk.maxwell.schema.columndef.ColumnDef;
import com.zendesk.maxwell.schema.columndef.ColumnDefCastException;
import com.zendesk.maxwell.scripting.Scripting;
import com.zendesk.maxwell.util.RunLoopProcess;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.StreamObserver;
import io.vitess.proto.Query;
import io.vitess.proto.Topodata;
import io.vitess.proto.Vtgate;
import io.vitess.proto.grpc.VitessGrpc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class VitessConnectorReplicator extends RunLoopProcess implements Replicator {
	static final Logger LOGGER = LoggerFactory.getLogger(VitessConnectorReplicator.class);
	private static final long MAX_TX_ELEMENTS = 10000;

	private final float bufferMemoryUsage;
	private final AbstractProducer producer;
	private final MaxwellOutputConfig outputConfig;

	private final SchemaStore schemaStore;
	private final Scripting scripting;
	private RowMapBuffer rowBuffer;


	private Histogram transactionRowCount;
	private Histogram transactionExecutionTime;
	private Long stopAtHeartbeat;
	private final String maxwellSchemaDatabaseName;

	private final Counter rowCounter;
	private final Meter rowMeter;

	private Position lastHeartbeatPosition;
	private String keyspace;
	protected VitessGrpc.VitessStub stub;

	protected StreamObserver<Vtgate.VStreamResponse> observer;
	protected Map<String, List<ColumnDef>> tableColumnsMap = new HashMap<>();


	public VitessConnectorReplicator(
			SchemaStore schemaStore,
			MaxwellMysqlConfig mysqlConfig,
			AbstractProducer producer,
			String maxwellSchemaDatabaseName,
			Metrics metrics,
			Position start,
			Scripting scripting,
			MaxwellOutputConfig outputConfig,
			float bufferMemoryUsage
	) {
		this.bufferMemoryUsage = bufferMemoryUsage;
		this.schemaStore = schemaStore;
		this.keyspace = mysqlConfig.database;
		this.scripting = scripting;
		this.outputConfig = outputConfig;
		this.maxwellSchemaDatabaseName = maxwellSchemaDatabaseName;
		this.lastHeartbeatPosition = start;
		this.producer = producer;

		/* setup metrics */
		rowCounter = metrics.getRegistry().counter(
				metrics.metricName("row", "count")
		);

		rowMeter = metrics.getRegistry().meter(
				metrics.metricName("row", "meter")
		);

		transactionRowCount = metrics.getRegistry().histogram(metrics.metricName("transaction", "row_count"));
		transactionExecutionTime = metrics.getRegistry().histogram(metrics.metricName("transaction", "execution_time"));

		AtomicReference<ManagedChannel> channel = new AtomicReference<>();
		ManagedChannel managedChannel = NettyChannelBuilder
				.forAddress(mysqlConfig.host, mysqlConfig.port)
				.usePlaintext()
				.build();
		channel.compareAndSet(null, managedChannel);
		this.stub = VitessGrpc.newStub(channel.get());
	}

	private boolean replicatorStarted = false;
	@Override
	public void startReplicator() throws Exception {

		this.observer = new StreamObserver<>() {

			@Override
			public void onNext(Vtgate.VStreamResponse response) {
				rowBuffer = new RowMapBuffer(MAX_TX_ELEMENTS, bufferMemoryUsage);

				LOGGER.info("Received {} vEvents in the VStreamResponse:",
						response.getEventsCount());

				Vgtid vgtid = getVgtid(response);

				RowMap rowMap;
				Position position = null;
				String changeType = "";
				List<String> pkFields = new ArrayList();
				List<ColumnDef> columnDefs = new ArrayList<>();
				HashMap<String, Object> newData = new HashMap<>();
				HashMap<String, Object> oldData = new HashMap<>();
				String[] schemaTableTuple = new String[2];
				long beginTs = Instant.now().toEpochMilli();
				for (int i = 0; i < response.getEventsCount(); i++) {

					Binlogdata.VEvent vEvent = response.getEvents(i);
					LOGGER.info("vEvent: {}", vEvent);

					Binlogdata.VEventType eventType = vEvent.getType();

					if (eventType == Binlogdata.VEventType.BEGIN) {
						beginTs = vEvent.getTimestamp();
						continue;
					} else if (eventType == Binlogdata.VEventType.COMMIT) {
						rowBuffer.getLast().setTXCommit();
						long timeSpent = vEvent.getTimestamp() - beginTs;
						transactionExecutionTime.update(timeSpent);
						transactionRowCount.update(rowBuffer.size());
						continue;
					} else if (eventType == Binlogdata.VEventType.FIELD) {
						// TODO: This needs to be processed via MysqlSchemaCompactor somehow
						schemaTableTuple = vEvent.getFieldEvent().getTableName().split("\\.");
						columnDefs = handleFieldEvent(vEvent);
						tableColumnsMap.put(schemaTableTuple[1], columnDefs);
						continue;
					} else if (eventType == Binlogdata.VEventType.VGTID) {
						position = handleVgtidEvent(rowBuffer, vEvent);
						lastHeartbeatPosition = position;
						continue;
					} else if (eventType == Binlogdata.VEventType.ROW) {
						schemaTableTuple = vEvent.getRowEvent().getTableName().split("\\.");

						for (Binlogdata.RowChange rowChange : vEvent.getRowEvent().getRowChangesList()
						) {
							Query.Row newRow = rowChange.getAfter();
							Query.Row oldRow = rowChange.getBefore();
							if (oldRow != Query.Row.getDefaultInstance() && newRow != Query.Row.getDefaultInstance()) {
								changeType = "update";
							} else if (oldRow != Query.Row.getDefaultInstance() && newRow == Query.Row.getDefaultInstance()) {
								changeType = "delete";
							} else if (oldRow == Query.Row.getDefaultInstance() && newRow != Query.Row.getDefaultInstance()) {
								changeType = "insert";
							} else {
								LOGGER.error("Unexpected RowChange: " + rowChange);
							}

							try {
								extractData(tableColumnsMap.get(schemaTableTuple[1]), newData, newRow);
								extractData(tableColumnsMap.get(schemaTableTuple[1]), oldData, oldRow);
							} catch (ColumnDefCastException e) {
								LOGGER.error("Unable to add column data: ", e);
							}

							LOGGER.info("OldRow : {}", oldRow);
							LOGGER.info("NewRow : {}", newRow);
							LOGGER.info("OldData : {}", oldData);
							LOGGER.info("NewData : {}", newData);

						}
					}
					// TODO: once schema tracking is working, we'll
					// have a better way to handle PKs
					columnDefs = tableColumnsMap.get(schemaTableTuple[1]);
					if (pkFields.size() < 1 && columnDefs != null && columnDefs.size() > 0) {
						pkFields.add(columnDefs.get(0).getName());
					}
					rowMap = new RowMap(
							changeType,
							schemaTableTuple[0],
							schemaTableTuple[1],
							vEvent.getTimestamp(),
							pkFields,
							position
					);
					for (Map.Entry<String, Object> entry : newData.entrySet()) {
						rowMap.putData(entry.getKey(), entry.getValue());
					}
					for (Map.Entry<String, Object> entry : oldData.entrySet()) {
						rowMap.putOldData(entry.getKey(), entry.getValue());
					}
					try {
						rowBuffer.add(rowMap);
					} catch (IOException e) {
						LOGGER.error("Unable to add row to buffer: " + e);
					}
					try {
						LOGGER.info("RowMap buffered: {}", rowMap.toJSON());
					} catch (Exception e) {
						LOGGER.error("Non-serializable RowMap: " + e);
					}
				}
				try {
					work();
				} catch (Exception e) {
					LOGGER.error("Unable to process row: " + e);
				}
			}

			@Override
			public void onError(Throwable t) {
				LOGGER.info("VStream streaming onError. Status: " + Status.fromThrowable(t), t);

			}

			@Override
			public void onCompleted() {
				LOGGER.info("VStream streaming completed.");
			}

		};
		String shardGtid = Vgtid.CURRENT_GTID;
		if (this.lastHeartbeatPosition != null) {
			shardGtid = this.lastHeartbeatPosition.getBinlogPosition().getGtidSetStr();
		}

		Vgtid vgtid = Vgtid.of(
				Binlogdata.VGtid.newBuilder()
						.addShardGtids(
								Binlogdata.ShardGtid.newBuilder()
										.setGtid(shardGtid)
										.setKeyspace(this.keyspace).build())
						.build());
		stub.vStream(
				Vtgate.VStreamRequest.newBuilder()
						.setVgtid(vgtid.getRawVgtid())
						.setTabletType(Topodata.TabletType.REPLICA)
						.build(), this.observer);
		replicatorStarted = true;
	}

	@Override
	public RowMap getRow() throws Exception {

		if ( !replicatorStarted ) {
			LOGGER.warn("replicator was not started, calling startReplicator()...");
			startReplicator();
		}

		while (true) {
			if (rowBuffer != null && !rowBuffer.isEmpty()) {
				RowMap row = rowBuffer.removeFirst();
				return row;
			}
		}
	}

	private List<ColumnDef> handleFieldEvent(Binlogdata.VEvent vEvent) {
		List<ColumnDef> columnDefs = new ArrayList<>();
		short pos = 0;
		for (Query.Field field : vEvent.getFieldEvent().getFieldsList()) {
			try {
				String columnType = field.getType().toString().toLowerCase(Locale.ROOT);
				String[] enumValues = new String[0];
				if (columnType.contains("int")) {
					columnType = "int";
				} else if (columnType.contains("float")) {
					columnType = "float";
				} else if (columnType.contains("enum")) {
					String ct = field.getColumnType();
					String[] split = ct.split("\\'");
					enumValues = Stream.of(split)
							.filter(Predicate.not(value -> value.startsWith("enum(")))
							.filter(Predicate.not(value -> value.equals(",")))
							.filter(Predicate.not(value -> value.equals(")")))
							.collect(Collectors.toSet()).toArray(new String[0]);
				}
				ColumnDef columnDef = ColumnDef.build(
						field.getName(),
						"utf8mb4",
						columnType,
						pos,
						false,
						enumValues,
						Long.valueOf(field.getColumnLength()));
				pos++;
				columnDefs.add(columnDef);
			} catch (IllegalArgumentException iae) {
				LOGGER.error("Column Definition build failed: " + iae);
				throw(iae);
			}
		}
		LOGGER.info("Column Definitions: {}", columnDefs);
		return columnDefs;
	}

	private Position handleVgtidEvent(RowMapBuffer buffer, Binlogdata.VEvent vEvent) {
		BinlogPosition binlogPosition = null;
		if (!buffer.isEmpty()) {
			buffer.getLast().setTXCommit();
		}
		for (Binlogdata.ShardGtid sgtid : vEvent.getVgtid().getShardGtidsList()) {
			binlogPosition = new BinlogPosition(
					sgtid.getGtid(),
					null,
					vEvent.getTimestamp(),
					sgtid.getShard()
			);
		}
		return new Position(binlogPosition, vEvent.getTimestamp());
	}

	private void extractData(List<ColumnDef> columnDefs, HashMap<String, Object> data, Query.Row row) throws ColumnDefCastException {
		int numberOfColumns = columnDefs.size();
		int rawValueIndex = 0;
		if ( row != Query.Row.getDefaultInstance() ) {

			for (short j = 0; j < numberOfColumns; j++) {
				ColumnDef column = columnDefs.get(j);
				final String columnName = column.getName();
				String rawValues = row.getValues().toStringUtf8();

				final int rawValueLength = (int) row.getLengths(j);
				final String rawValue = rawValueLength == -1
						? null
						: new String(Arrays.copyOfRange(rawValues.getBytes(StandardCharsets.UTF_8), rawValueIndex, rawValueIndex + rawValueLength));
				if (rawValueLength != -1) {
					// no update to rawValueIndex when no value in the rawValue
					rawValueIndex += rawValueLength;
				}
				if (rawValue == null) {
					data.put(columnName, rawValue);
				} else {
					data.put(columnName, column.asJSON(rawValue, outputConfig));
				}
			}
		}
	}

	@Override
	public Long getLastHeartbeatRead() {
		return lastHeartbeatPosition.getLastHeartbeatRead();
	}

	@Override
	public Schema getSchema() throws SchemaStoreException {
		return this.schemaStore.getSchema();
	}

	@Override
	public Long getSchemaId() throws SchemaStoreException {
		return this.schemaStore.getSchemaID();
	}

	@Override
	protected void work() throws Exception {
		if (! replicatorStarted ) {
			startReplicator();
		}

		RowMap row = null;
		try {
			row = getRow();
		} catch ( InterruptedException e ) {
			LOGGER.error("Unable to getRow: " + e);
		}

		if ( row == null )
			return;

		rowCounter.inc();
		rowMeter.mark();

		if ( scripting != null && !isMaxwellRow(row))
			scripting.invoke(row);

		processRow(row);
	}

	public void stopAtHeartbeat(long heartbeat) {
		stopAtHeartbeat = heartbeat;
	}

	protected void processRow(RowMap row) throws Exception {
		producer.push(row);
	}

	// We assume there is at most one vgtid event for response.
	// Even in case of resharding, there is only one vgtid event that contains multiple shard
	// gtids.
	private Vgtid getVgtid(Vtgate.VStreamResponse response) {
		LinkedList<Vgtid> vgtids = new LinkedList<>();
		for (Binlogdata.VEvent vEvent : response.getEventsList()) {
			if (vEvent.getType() == Binlogdata.VEventType.VGTID) {
				vgtids.addLast(Vgtid.of(vEvent.getVgtid()));
			}
		}
		if (vgtids.size() == 0) {
			// The VStreamResponse that contains an VERSION vEvent does not have VGTID.
			// We do not update lastReceivedVgtid in this case.
			// It can also be null if the 1st grpc response does not have vgtid upon restart
			LOGGER.info("No vgtid found in response {}...", response.toString().substring(0, Math.min(100, response.toString().length())));
			LOGGER.info("Full response is {}", response);
			return Vgtid.of(Binlogdata.VGtid.newBuilder()
					.addShardGtids(
							Binlogdata.ShardGtid.newBuilder()
									.setKeyspace(this.keyspace)
									.setGtid(Vgtid.CURRENT_GTID)
									.build()).build());
		}
		if (vgtids.size() > 1) {
			LOGGER.error(
					"Should only have 1 vgtid per VStreamResponse, but found {}. Use the last vgtid {}.",
					vgtids.size(), vgtids.getLast());
		}
		return vgtids.getLast();
	}

	private int getNumOfRowEvents(Vtgate.VStreamResponse response) {
		int num = 0;
		for (Binlogdata.VEvent vEvent : response.getEventsList()) {
			if (vEvent.getType() == Binlogdata.VEventType.ROW) {
				num++;
			}
		}
		return num;
	}

	protected boolean isMaxwellRow(RowMap row) {
		return row.getDatabase().equals(this.maxwellSchemaDatabaseName);
	}
}
