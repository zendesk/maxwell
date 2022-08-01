package com.zendesk.maxwell.replication;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.QueryEventData;
import com.github.shyiko.mysql.binlog.event.RowsQueryEventData;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import com.github.shyiko.mysql.binlog.network.ServerException;
import com.zendesk.maxwell.MaxwellMysqlConfig;
import com.zendesk.maxwell.bootstrap.BootstrapController;
import com.zendesk.maxwell.monitoring.Metrics;
import com.zendesk.maxwell.producer.AbstractProducer;
import com.zendesk.maxwell.producer.MaxwellOutputConfig;
import com.zendesk.maxwell.row.HeartbeatRowMap;
import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.row.RowMapBuffer;
import com.zendesk.maxwell.schema.Schema;
import com.zendesk.maxwell.schema.SchemaStoreException;
import com.zendesk.maxwell.scripting.Scripting;
import com.zendesk.maxwell.util.RunLoopProcess;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.zendesk.maxwell.replication.BinlogConnectorEventProcessor.CREATE_TABLE_PATTERN;

public class BinlogConnectorReplicator extends RunLoopProcess implements Replicator, BinaryLogClient.LifecycleListener {
	static final Logger LOGGER = LoggerFactory.getLogger(BinlogConnectorReplicator.class);
	private static final long MAX_TX_ELEMENTS = 10000;
	public static int BINLOG_QUEUE_SIZE = 5000;
	public static final int BAD_BINLOG_ERROR_CODE = 1236;
	public static final int ACCESS_DENIED_ERROR_CODE = 1227;

	private final String clientID;
	private final String maxwellSchemaDatabaseName;

	protected final BinaryLogClient client;
	private final int replicationReconnectionRetries;
	private final BinlogConnectorEventListener binlogEventListener;
	private BinlogConnectorLivenessMonitor binlogLivenessMonitor;
	private final LinkedBlockingDeque<BinlogConnectorEvent> queue;
	private final BinlogConnectorEventProcessor processor;
	private final Scripting scripting;
	private ServerException lastCommError;

	private final boolean stopOnEOF;
	private boolean hitEOF = false;

	private Long stopAtHeartbeat;

	private final BootstrapController bootstrapper;
	private final AbstractProducer producer;
	private RowMapBuffer rowBuffer;
	private final float bufferMemoryUsage;

	private final Counter rowCounter;
	private final Meter rowMeter;

	private final Histogram transactionRowCount;
	private final Histogram transactionExecutionTime;

	private final Boolean gtidPositioning;

	private boolean isConnected = false;

	private static class ClientReconnectedException extends Exception {}

	public BinlogConnectorReplicator(
		AbstractProducer producer,
		BootstrapController bootstrapper,
		MaxwellMysqlConfig mysqlConfig,
		Long replicaServerID,
		String maxwellSchemaDatabaseName,
		Metrics metrics,
		Position start,
		boolean stopOnEOF,
		String clientID,
		Scripting scripting,
		MaxwellOutputConfig outputConfig,
		BinlogConnectorEventProcessor processor,
		float bufferMemoryUsage,
		int replicationReconnectionRetries,
		int binlogEventQueueSize
	) {
		this.clientID = clientID;
		this.bootstrapper = bootstrapper;
		this.maxwellSchemaDatabaseName = maxwellSchemaDatabaseName;
		this.producer = producer;
		this.stopOnEOF = stopOnEOF;
		this.scripting = scripting;

		this.lastCommError = null;
		this.bufferMemoryUsage = bufferMemoryUsage;
		this.queue = new LinkedBlockingDeque<>(binlogEventQueueSize);
		this.processor = processor;

		/* setup metrics */
		rowCounter = metrics.getRegistry().counter(
			metrics.metricName("row", "count")
		);

		rowMeter = metrics.getRegistry().meter(
			metrics.metricName("row", "meter")
		);

		transactionRowCount = metrics.getRegistry().histogram(metrics.metricName("transaction", "row_count"));
		transactionExecutionTime = metrics.getRegistry().histogram(metrics.metricName("transaction", "execution_time"));

		/* setup binlog client */
		this.client = new BinaryLogClient(mysqlConfig.host, mysqlConfig.port, mysqlConfig.user, mysqlConfig.password);
		this.client.setSSLMode(mysqlConfig.sslMode);


		BinlogPosition startBinlog = start.getBinlogPosition();
		if (startBinlog.getGtidSetStr() != null) {
			String gtidStr = startBinlog.getGtidSetStr();
			LOGGER.info("Setting initial gtid to: " + gtidStr);
			this.client.setGtidSet(gtidStr);
			this.gtidPositioning = true;
		} else {
			LOGGER.info("Setting initial binlog pos to: " + startBinlog.getFile() + ":" + startBinlog.getOffset());
			this.client.setBinlogFilename(startBinlog.getFile());
			this.client.setBinlogPosition(startBinlog.getOffset());
			this.gtidPositioning = false;
		}

		/*
			for the moment, the reconnection code in keep-alive is broken;
			it sends along a binlog file as well as the GTID set,
			which triggers mysql to jump ahead a binlog.
			At some point I presume shyko will fix it and we can remove this.
		 */
		this.client.setKeepAlive(false);
		if (mysqlConfig.enableHeartbeat) {
			this.binlogLivenessMonitor = new BinlogConnectorLivenessMonitor(client);
			this.client.registerLifecycleListener(this.binlogLivenessMonitor);
			this.client.registerEventListener(this.binlogLivenessMonitor);
		}

		EventDeserializer eventDeserializer = new EventDeserializer();
		eventDeserializer.setCompatibilityMode(
			EventDeserializer.CompatibilityMode.DATE_AND_TIME_AS_LONG_MICRO,
			EventDeserializer.CompatibilityMode.CHAR_AND_BINARY_AS_BYTE_ARRAY,
			EventDeserializer.CompatibilityMode.INVALID_DATE_AND_TIME_AS_MIN_VALUE
		);
		this.client.setEventDeserializer(eventDeserializer);
		this.binlogEventListener = new BinlogConnectorEventListener(client, queue, metrics, outputConfig);
		this.client.setBlocking(!stopOnEOF);
		this.client.registerEventListener(binlogEventListener);
		this.client.registerLifecycleListener(this);
		this.client.setServerId(replicaServerID.intValue());

		this.replicationReconnectionRetries = replicationReconnectionRetries;
	}

	/**
	 * get a single row from the replicator and pass it to the producer or bootstrapper.
	 *
	 * This is the top-level function in the run-loop.
	 */
	public void work() throws Exception {
		RowMap row = null;
		try {
			row = getRow();
		} catch (InterruptedException ignored) {}

		if ( row == null )
			return;

		rowCounter.inc();
		rowMeter.mark();

		if ( scripting != null && !isMaxwellRow(row))
			scripting.invoke(row);

		processRow(row);
	}

	private boolean replicatorStarted = false;
	public void startReplicator() throws Exception {
		this.client.connect(5000);
		replicatorStarted = true;
	}

	@Override
	protected void beforeStop() throws Exception {
		this.binlogEventListener.stop();
		this.client.disconnect();
	}

	/**
	 * Listener for communication errors so we can stop everything and exit on this case
	 * @param ex Exception thrown by the BinaryLogClient
	 */
	@Override
	public void onCommunicationFailure(BinaryLogClient client, Exception ex) {
		LOGGER.warn("communications failure in binlog:", ex);

		// Stopping Maxwell only in case we cannot read binlogs from the current server
		if (ex instanceof ServerException) {
			ServerException serverEx = (ServerException) ex;
			int errCode = serverEx.getErrorCode();

			switch(errCode) {
				case BAD_BINLOG_ERROR_CODE:
				case ACCESS_DENIED_ERROR_CODE:
					lastCommError = serverEx;
				default:
					LOGGER.debug("error code: {} from server", errCode);
			}
		}
	}

	/**
	 * Get the last heartbeat that the replicator has processed.
	 *
	 * We pass along the value of the heartbeat to the producer inside the row map.
	 * @return the millisecond value ot the last heartbeat read
	 */

	public Long getLastHeartbeatRead() {
		return processor.getLastHeartbeatRead();
	}

	public void stopAtHeartbeat(long heartbeat) {
		stopAtHeartbeat = heartbeat;
	}

	/**
	 * Checks if any communications errors in the last update loop.
	 * @throws ServerException with the details of the communication error.
	 */
	private void checkCommErrors() throws ServerException {
		if (lastCommError != null) {
			throw lastCommError;
		}
	}

	/**
	 * Returns true if connected with a recent event.
	 * If binlog heartbeats are disabled, just returns
	 * whether there is a connection.
	 */
	private boolean isConnectionAlive() {
		if (!isConnected) {
			return false;
		}
		return this.binlogLivenessMonitor == null || binlogLivenessMonitor.isAlive();
	}

	private boolean shouldSkipRow(RowMap row) throws IOException {
		if ( isMaxwellRow(row) && !isBootstrapInsert(row))
			return true;

		/* NOTE: bootstrapper.shouldSkip will block us if
		   we're in synchronous bootstrapping mode.  It also
		   has the side affect of taking the row into a queue if
		   we're in async bootstrapping mode */
		return bootstrapper != null && bootstrapper.shouldSkip(row);
	}

	protected void processRow(RowMap row) throws Exception {
		if ( row instanceof HeartbeatRowMap) {
			producer.push(row);
			if (stopAtHeartbeat != null) {
				long thisHeartbeat = row.getPosition().getLastHeartbeatRead();
				if (thisHeartbeat >= stopAtHeartbeat) {
					LOGGER.info("received final heartbeat " + thisHeartbeat + "; stopping replicator");
					// terminate runLoop
					this.taskState.stopped();
				}
			}
		} else if ( !shouldSkipRow(row) )
			producer.push(row);
	}

	/**
	 * Is this RowMap an update to one of maxwell's own tables?
	 *
	 * We don't output updates to maxwell.positions, and updates to maxwell.heartbeats
	 * are always treated specially.
	 *
	 * @param row The RowMap in question
	 * @return whether the update is something maxwell itself generated
	 */
	protected boolean isMaxwellRow(RowMap row) {
		return row.getDatabase().equals(this.maxwellSchemaDatabaseName);
	}

	private boolean isBootstrapInsert(RowMap row) {
		return row.getDatabase().equals(this.maxwellSchemaDatabaseName)
			&& row.getRowType().equals("insert")
			&& row.getTable().equals("bootstrap");
	}

	private void ensureReplicatorThread() throws Exception {
		checkCommErrors();
		if (!this.isConnected && stopOnEOF) {
			// reached EOF, nothing to do
			return;
		}
		if (!this.isConnectionAlive()) {
			client.disconnect();
			if (this.gtidPositioning) {
				// When using gtid positioning, reconnecting should take us to the top
				// of the gtid event.  We throw away any binlog position we have
				// (other than GTID) and bail out of getTransactionRows()

				LOGGER.warn("replicator stopped at position: {} -- restarting", client.getGtidSet());

				client.setBinlogFilename("");
				client.setBinlogPosition(4L);
				tryReconnect();

				throw new ClientReconnectedException();
			} else {
				// standard binlog positioning is a lot easier; we can really reconnect anywhere
				// we like, so we don't have to bail out of the middle of an event.
				LOGGER.warn("replicator stopped at position: {} -- restarting", client.getBinlogFilename() + ":" + client.getBinlogPosition());

				long oldMasterId = client.getMasterServerId();
				tryReconnect();
				if (client.getMasterServerId() != oldMasterId) {
					throw new Exception("Master id changed from " + oldMasterId + " to " + client.getMasterServerId()
								+ " while using binlog coordinate positioning. Cannot continue with the info that we have");
				}
			}
		}
	}

	private void tryReconnect() throws TimeoutException {
		int reconnectionAttempts = 0;

		while ((reconnectionAttempts += 1) <= this.replicationReconnectionRetries || this.replicationReconnectionRetries == 0) {
			try {
				LOGGER.info(String.format("Reconnection attempt: %s of %s", reconnectionAttempts, replicationReconnectionRetries > 0 ? this.replicationReconnectionRetries : "unlimited"));
				client.connect(5000);
				return;
			} catch (IOException | TimeoutException ignored) { }
		}
		throw new TimeoutException("Maximum reconnection attempts reached.");
	}

	/**
	 * Get a batch of rows for the current transaction.
	 *
	 * We assume the replicator has just processed a "BEGIN" event, and now
	 * we're inside a transaction.  We'll process all rows inside that transaction
	 * and turn them into RowMap objects.  We do this because mysql attaches the
	 * transaction-id (xid) to the COMMIT event (at the end of the transaction),
	 * so we process the entire transaction in order to assign each row the same xid.

	 * @return A RowMapBuffer of rows; either in-memory or on disk.
	 */

	private RowMapBuffer getTransactionRows(BinlogConnectorEvent beginEvent) throws Exception {
		BinlogConnectorEvent event;
		RowMapBuffer buffer = new RowMapBuffer(MAX_TX_ELEMENTS, this.bufferMemoryUsage);

		String currentQuery = null;

		while ( true ) {
			event = pollEvent();

			if (event == null) {
				ensureReplicatorThread();
				continue;
			}

			EventType eventType = event.getEvent().getHeader().getEventType();
			if (event.isCommitEvent()) {
				if (!buffer.isEmpty()) {
					buffer.getLast().setTXCommit();
					long timeSpent = buffer.getLast().getTimestampMillis() - beginEvent.getEvent().getHeader().getTimestamp();
					transactionExecutionTime.update(timeSpent);
					transactionRowCount.update(buffer.size());
				}
				if(eventType == EventType.XID) {
					buffer.setXid(event.xidData().getXid());
				}
				return buffer;
			}

			switch(eventType) {
				case WRITE_ROWS:
				case UPDATE_ROWS:
				case DELETE_ROWS:
				case EXT_WRITE_ROWS:
				case EXT_UPDATE_ROWS:
				case EXT_DELETE_ROWS:
					processor.writeRow(event, buffer, currentQuery);
					currentQuery = null;
					break;
				case TABLE_MAP:
					processor.cacheTable(event.tableMapData());
					break;
				case ROWS_QUERY:
					RowsQueryEventData rqed = event.getEvent().getData();
					currentQuery = rqed.getQuery();
					break;
				case QUERY:
					QueryEventData qe = event.queryData();
					String sql = qe.getSql();
					String upperCaseSql = sql.toUpperCase();

					if (upperCaseSql.startsWith(BinlogConnectorEvent.SAVEPOINT)) {
						LOGGER.debug("Ignoring SAVEPOINT in transaction: {}", qe);
					} else if (CREATE_TABLE_PATTERN.matcher(sql).find()) {
						// CREATE TABLE `foo` SELECT * FROM `bar` will put a CREATE TABLE
						// inside a transaction.  Note that this could, in rare cases, lead
						// to us starting on a WRITE_ROWS event -- we sync the schema position somewhere
						// kinda unsafe.
						if (bootstrapper != null)
							bootstrapper.setCurrentSchemaID(processor.getSchemaId());
						processor.processQueryEvent(event, producer);
					} else if (upperCaseSql.startsWith("INSERT INTO MYSQL.RDS_") || upperCaseSql.startsWith("DELETE FROM MYSQL.RDS_")) {
						// RDS heartbeat events take the following form:
						// INSERT INTO mysql.rds_heartbeat2(id, value) values (1,1483041015005) ON DUPLICATE KEY UPDATE value = 1483041015005

						// Other RDS internal events like below:
						// INSERT INTO mysql.rds_sysinfo(name, value) values ('innodb_txn_key','Thu Nov 15 10:30:07 UTC 2018')
						// DELETE FROM mysql.rds_sysinfo where name = 'innodb_txn_key'

						// We don't need to process them, just ignore
					} else if (upperCaseSql.startsWith("DROP TEMPORARY TABLE")) {
						// Ignore temporary table drop statements inside transactions
					} else if ( upperCaseSql.startsWith("# DUMMY EVENT")) {
						// MariaDB injected event
					} else {
						LOGGER.warn("Unhandled QueryEvent @ {} inside transaction: {}", event.getPosition().fullPosition(), qe);
					}
					break;
			}
		}
	}


	/**
	 * The main entry point into the event reading loop.
	 * <p>
	 * We maintain a buffer of events in a transaction,
	 * and each subsequent call to `getRow` can grab one from
	 * the buffer.  If that buffer is empty, we'll go check
	 * the open-replicator buffer for rows to process.  If that
	 * buffer is empty, we return null.
	 * </p>
	 * @return either a RowMap or null
	 */
	public RowMap getRow() throws Exception {
		BinlogConnectorEvent event;

		if ( stopOnEOF && hitEOF )
			return null;

		if ( !replicatorStarted ) {
			LOGGER.warn("replicator was not started, calling startReplicator()...");
			startReplicator();
		}

		while (true) {
			if (rowBuffer != null && !rowBuffer.isEmpty()) {
				RowMap row = rowBuffer.removeFirst();

				if (row != null && isMaxwellRow(row) && row.getTable().equals("heartbeats"))
					return processor.processHeartbeats(row, clientID);
				else
					return row;
			}

			event = pollEvent();

			if (event == null) {
				if ( stopOnEOF ) {
					if ( this.isConnected )
						continue;
					else
						return null;
				} else {
					try {
						ensureReplicatorThread();
					} catch (ClientReconnectedException ignored) {
					}
					return null;
				}
			}

			switch (event.getType()) {
				case WRITE_ROWS:
				case EXT_WRITE_ROWS:
				case UPDATE_ROWS:
				case EXT_UPDATE_ROWS:
				case DELETE_ROWS:
				case EXT_DELETE_ROWS:
					LOGGER.warn("Started replication stream inside a transaction.  This shouldn't normally happen.");
					LOGGER.warn("Assuming new transaction at unexpected event:" + event);

					queue.offerFirst(event);
					rowBuffer = getTransactionRows(event);
					break;
				case TABLE_MAP:
					processor.cacheTable(event.tableMapData());
					break;
				case QUERY:
					QueryEventData qe = event.queryData();
					String sql = qe.getSql();
					if (BinlogConnectorEvent.BEGIN.equals(sql)) {
						try {
							rowBuffer = getTransactionRows(event);
						} catch (ClientReconnectedException e) {
							// rowBuffer should already be empty by the time we get to this switch
							// statement, but we null it for clarity
							rowBuffer = null;
							break;
						}
						rowBuffer.setServerId(event.getEvent().getHeader().getServerId());
						rowBuffer.setThreadId(qe.getThreadId());
						rowBuffer.setSchemaId(getSchemaId());
					} else {
						processor.processQueryEvent(event, producer);
					}
					break;
				case ROTATE:
					processor.clearTableCache();
					if (stopOnEOF && event.getPosition().getOffset() > 0) {
						this.binlogEventListener.mustStop.set(true);
						this.client.disconnect();
						this.hitEOF = true;
						return null;
					}
					break;
				default:
					break;
			}

		}
	}

	protected BinlogConnectorEvent pollEvent() throws InterruptedException {
		return queue.poll(100, TimeUnit.MILLISECONDS);
	}

	public Schema getSchema() throws SchemaStoreException {
		return this.processor.getSchema();
	}

	public Long getSchemaId() throws SchemaStoreException {
		return this.processor.getSchemaId();
	}

	@Override
	public void onConnect(BinaryLogClient client) {
		LOGGER.info("Binlog connected.");
		this.isConnected = true;
	}

	@Override
	public void onEventDeserializationFailure(BinaryLogClient client, Exception ex) {
		LOGGER.warn("Event deserialization failure.", ex);
		LOGGER.warn("cause: ", ex.getCause());
	}

	@Override
	public void onDisconnect(BinaryLogClient client) {
		LOGGER.info("Binlog disconnected.");
		this.isConnected = false;
	}


}
