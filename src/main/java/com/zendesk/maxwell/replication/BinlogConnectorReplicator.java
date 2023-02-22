package com.zendesk.maxwell.replication;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.AnnotateRowsEventData;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.MariadbGtidEventData;
import com.github.shyiko.mysql.binlog.event.QueryEventData;
import com.github.shyiko.mysql.binlog.event.RowsQueryEventData;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import com.github.shyiko.mysql.binlog.network.ServerException;
import com.zendesk.maxwell.MaxwellMysqlConfig;
import com.zendesk.maxwell.bootstrap.BootstrapController;
import com.zendesk.maxwell.filtering.Filter;
import com.zendesk.maxwell.monitoring.Metrics;
import com.zendesk.maxwell.producer.AbstractProducer;
import com.zendesk.maxwell.producer.MaxwellOutputConfig;
import com.zendesk.maxwell.row.HeartbeatRowMap;
import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.row.RowMapBuffer;
import com.zendesk.maxwell.schema.*;
import com.zendesk.maxwell.schema.columndef.ColumnDefCastException;
import com.zendesk.maxwell.schema.ddl.DDLMap;
import com.zendesk.maxwell.schema.ddl.ResolvedSchemaChange;
import com.zendesk.maxwell.scripting.Scripting;
import com.zendesk.maxwell.util.RunLoopProcess;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;

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
	private BinlogConnectorEventListener binlogEventListener;
	private BinlogConnectorLivenessMonitor binlogLivenessMonitor;
	private final LinkedBlockingDeque<BinlogConnectorEvent> queue;
	private final TableCache tableCache;
	private final Scripting scripting;
	private ServerException lastCommError;

	private final boolean stopOnEOF;
	private boolean hitEOF = false;

	private Position lastHeartbeatPosition;
	private final HeartbeatNotifier heartbeatNotifier;
	private Long stopAtHeartbeat;
	private Filter filter;
	private Boolean ignoreMissingSchema;

	private final BootstrapController bootstrapper;
	private final AbstractProducer producer;
	private RowMapBuffer rowBuffer;
	private final float bufferMemoryUsage;

	private final Counter rowCounter;
	private final Meter rowMeter;

	private SchemaStore schemaStore;
	private Histogram transactionRowCount;
	private Histogram transactionExecutionTime;

	private final Boolean gtidPositioning;

	private static Pattern createTablePattern =
		Pattern.compile("^CREATE\\s+TABLE", Pattern.CASE_INSENSITIVE);

	private boolean isConnected = false;

	private class ClientReconnectedException extends Exception {}

	public BinlogConnectorReplicator(
			SchemaStore schemaStore,
			AbstractProducer producer,
			BootstrapController bootstrapper,
			MaxwellMysqlConfig mysqlConfig,
			Long replicaServerID,
			String maxwellSchemaDatabaseName,
			Metrics metrics,
			Position start,
			boolean stopOnEOF,
			String clientID,
			HeartbeatNotifier heartbeatNotifier,
			Scripting scripting,
			Filter filter,
			MaxwellOutputConfig outputConfig,
			float bufferMemoryUsage,
			int replicationReconnectionRetries
	) {
		this(
				schemaStore,
				producer,
				bootstrapper,
				mysqlConfig,
				replicaServerID,
				maxwellSchemaDatabaseName,
				metrics,
				start,
				stopOnEOF,
				clientID,
				heartbeatNotifier,
				scripting,
				filter,
				false,
				outputConfig,
				bufferMemoryUsage,
				replicationReconnectionRetries,
				BINLOG_QUEUE_SIZE
		);
	}

	public BinlogConnectorReplicator(
		SchemaStore schemaStore,
		AbstractProducer producer,
		BootstrapController bootstrapper,
		MaxwellMysqlConfig mysqlConfig,
		Long replicaServerID,
		String maxwellSchemaDatabaseName,
		Metrics metrics,
		Position start,
		boolean stopOnEOF,
		String clientID,
		HeartbeatNotifier heartbeatNotifier,
		Scripting scripting,
		Filter filter,
		boolean ignoreMissingSchema,
		MaxwellOutputConfig outputConfig,
		float bufferMemoryUsage,
		int replicationReconnectionRetries,
		int binlogEventQueueSize
	) {
		this.clientID = clientID;
		this.bootstrapper = bootstrapper;
		this.maxwellSchemaDatabaseName = maxwellSchemaDatabaseName;
		this.producer = producer;
		this.lastHeartbeatPosition = start;
		this.heartbeatNotifier = heartbeatNotifier;
		this.stopOnEOF = stopOnEOF;
		this.scripting = scripting;
		this.schemaStore = schemaStore;
		this.tableCache = new TableCache(maxwellSchemaDatabaseName);
		this.filter = filter;
		this.ignoreMissingSchema = ignoreMissingSchema;
		this.lastCommError = null;
		this.bufferMemoryUsage = bufferMemoryUsage;
		this.queue = new LinkedBlockingDeque<>(binlogEventQueueSize);

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
		this.client.setUseSendAnnotateRowsEvent(true);


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
		} catch ( InterruptedException e ) {
		}

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
		return lastHeartbeatPosition.getLastHeartbeatRead();
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
		if ( bootstrapper != null && bootstrapper.shouldSkip(row) )
			return true;

		return false;
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
	 * If the input RowMap is one of the heartbeat pulses we sent out,
	 * process it.  If it's one of our heartbeats, we build a `HeartbeatRowMap`,
	 * which will be handled specially in producers (namely, it causes the binlog position to advance).
	 * It is isn't, we leave the row as a RowMap and the rest of the chain will ignore it.
	 *
	 * @return either a RowMap or a HeartbeatRowMap
	 */

	private RowMap processHeartbeats(RowMap row) {
		String hbClientID = (String) row.getData("client_id");
		if ( !Objects.equals(hbClientID, this.clientID) )
			return row; // plain row -- do not process.

		long lastHeartbeatRead = (Long) row.getData("heartbeat");
		LOGGER.debug("replicator picked up heartbeat: {}", lastHeartbeatRead);
		this.lastHeartbeatPosition = row.getPosition().withHeartbeat(lastHeartbeatRead);
		heartbeatNotifier.heartbeat(lastHeartbeatRead);
		return HeartbeatRowMap.valueOf(row.getDatabase(), this.lastHeartbeatPosition, row.getNextPosition().withHeartbeat(lastHeartbeatRead));
	}

	/**
	 * Parse a DDL statement and output the results to the producer
	 *
	 * @param dbName The database "context" under which the SQL is to be processed.  think "use db; alter table foo ..."
	 * @param sql The DDL SQL to be processed
	 * @param schemaStore A SchemaStore object to which we delegate the parsing of the sql
	 * @param position The position that the SQL happened at
	 * @param timestamp The timestamp of the SQL binlog event
	 */
	private void processQueryEvent(String dbName, String sql, SchemaStore schemaStore, Position position, Position nextPosition, Long timestamp) throws Exception {
		List<ResolvedSchemaChange> changes = schemaStore.processSQL(sql, dbName, position);
		Long schemaId = getSchemaId();

		if ( bootstrapper != null)
			bootstrapper.setCurrentSchemaID(schemaId);

		for (ResolvedSchemaChange change : changes) {
			if (change.shouldOutput(filter)) {
				DDLMap ddl = new DDLMap(change, timestamp, sql, position, nextPosition, schemaId);

				if ( scripting != null )
					scripting.invoke(ddl);

				producer.push(ddl);
			}
		}

		tableCache.clear();
	}

	private void processQueryEvent(BinlogConnectorEvent event) throws Exception {
		QueryEventData data = event.queryData();
		processQueryEvent(
			data.getDatabase(),
			data.getSql(),
			this.schemaStore,
			Position.valueOf(event.getPosition(), getLastHeartbeatRead()),
			Position.valueOf(event.getNextPosition(), getLastHeartbeatRead()),
			event.getEvent().getHeader().getTimestamp()
		);
	}

	/**
	 * Should we output a batch of rows for the given database and table?
	 *
	 * First against a whitelist/blacklist/filter.  The whitelist
	 * ensures events that maxwell needs (maxwell.bootstrap, maxwell.heartbeats)
	 * are always passed along.
	 *
	 * The system the blacklist gets rid of the
	 * `ha_health_check` and `rds_heartbeat` tables which are weird
	 * replication-control mechanism events in Alibaba RDS (and maybe amazon?)
	 *
	 * Then we check the configured filters.
	 *
	 * Finally, if we decide to exclude a table we check the filter to
	 * see if it's possible that a column-value filter could reverse this decision
	 *
	 * @param database The database of the DML
	 * @param table The table of the DML
	 * @param filter A table-filter, or null
	 * @param columnNames Names of the columns this table contains
	 * @return Whether we should write the event to the producer
	 */
	private boolean shouldOutputEvent(String database, String table, Filter filter, Set<String> columnNames) {
		if ( Filter.isSystemBlacklisted(database, table) )
			return false;
		else if ( filter.isSystemWhitelisted(database, table) )
			return true;
		else {
			if ( Filter.includes(filter, database, table) )
				return true;
			else
				return Filter.couldIncludeFromColumnFilters(filter, database, table, columnNames);
		}
	}


	private boolean shouldOutputRowMap(String database, String table, RowMap rowMap, Filter filter) {
		return filter.isSystemWhitelisted(database, table) ||
			filter.includes(database, table, rowMap.getData());
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

				Long oldMasterId = client.getMasterServerId();
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
					Table table = tableCache.getTable(event.getTableID());

					if ( table != null && shouldOutputEvent(table.getDatabase(), table.getName(), filter, table.getColumnNames()) ) {
						List<RowMap> rows;
						try {
							rows = event.jsonMaps(table, getLastHeartbeatRead(), currentQuery);
						} catch ( ColumnDefCastException e ) {
							logColumnDefCastException(table, e);

							throw(e);
						}

						for ( RowMap r : rows )
							if (shouldOutputRowMap(table.getDatabase(), table.getName(), r, filter)) {
								buffer.add(r);
							}
					}
					break;
				case TABLE_MAP:
					TableMapEventData data = event.tableMapData();
					tableCache.processEvent(getSchema(), this.filter, this.ignoreMissingSchema, data.getTableId(), data.getDatabase(), data.getTable());
					break;
				case ROWS_QUERY:
					RowsQueryEventData rqed = event.getEvent().getData();
					currentQuery = rqed.getQuery();
					break;
				case ANNOTATE_ROWS:
					AnnotateRowsEventData ared = event.getEvent().getData();
					currentQuery = ared.getRowsQuery();
					break;
				case QUERY:
					QueryEventData qe = event.queryData();
					String sql = qe.getSql();
					String upperCaseSql = sql.toUpperCase();

					if ( upperCaseSql.startsWith(BinlogConnectorEvent.SAVEPOINT)) {
						LOGGER.debug("Ignoring SAVEPOINT in transaction: {}", qe);
					} else if ( createTablePattern.matcher(sql).find() ) {
						// CREATE TABLE `foo` SELECT * FROM `bar` will put a CREATE TABLE
						// inside a transaction.  Note that this could, in rare cases, lead
						// to us starting on a WRITE_ROWS event -- we sync the schema position somewhere
						// kinda unsafe.
						processQueryEvent(event);
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
				default:
					break;
			}
		}
	}

	private void logColumnDefCastException(Table table, ColumnDefCastException e) {
		String castInfo = String.format(
				"Unable to cast %s (%s) into column %s.%s.%s (type '%s')",
				e.givenValue.toString(),
				e.givenValue.getClass().getName(),
				table.getDatabase(),
				table.getName(),
				e.def.getName(),
				e.def.getType()
		);
		LOGGER.error(castInfo);

		e.database = table.getDatabase();
		e.table = table.getName();
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

				if ( row != null && isMaxwellRow(row) && row.getTable().equals("heartbeats") )
					return processHeartbeats(row);
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
					} catch ( ClientReconnectedException e ) {}
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
					TableMapEventData data = event.tableMapData();
					tableCache.processEvent(getSchema(), this.filter,this.ignoreMissingSchema, data.getTableId(), data.getDatabase(), data.getTable());
					break;
				case QUERY:
					QueryEventData qe = event.queryData();
					String sql = qe.getSql();
					if (BinlogConnectorEvent.BEGIN.equals(sql)) {
						try {
							rowBuffer = getTransactionRows(event);
						} catch ( ClientReconnectedException e ) {
							// rowBuffer should already be empty by the time we get to this switch
							// statement, but we null it for clarity
							rowBuffer = null;
							break;
						}
						rowBuffer.setServerId(event.getEvent().getHeader().getServerId());
						rowBuffer.setThreadId(qe.getThreadId());
						rowBuffer.setSchemaId(getSchemaId());
					} else {
						processQueryEvent(event);
					}
					break;
				case MARIADB_GTID:
					// in mariaDB the GTID event supplants the normal BEGIN
					MariadbGtidEventData g = event.mariaGtidData();
					if ( (g.getFlags() & MariadbGtidEventData.FL_STANDALONE) == 0 ) {
						try {
							rowBuffer = getTransactionRows(event);
						} catch ( ClientReconnectedException e ) {
							// rowBuffer should already be empty by the time we get to this switch
							// statement, but we null it for clarity
							rowBuffer = null;
							break;
						}
						rowBuffer.setServerId(event.getEvent().getHeader().getServerId());
						rowBuffer.setSchemaId(getSchemaId());
					}
					break;
				case ROTATE:
					tableCache.clear();
					if ( stopOnEOF && event.getPosition().getOffset() > 0 ) {
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
		return this.schemaStore.getSchema();
	}

	public Long getSchemaId() throws SchemaStoreException {
		return this.schemaStore.getSchemaID();
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
