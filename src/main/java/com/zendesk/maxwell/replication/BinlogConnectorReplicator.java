package com.zendesk.maxwell.replication;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.QueryEventData;
import com.github.shyiko.mysql.binlog.event.RowsQueryEventData;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.MaxwellMysqlConfig;
import com.zendesk.maxwell.scripting.Scripting;
import com.zendesk.maxwell.bootstrap.AbstractBootstrapper;
import com.zendesk.maxwell.filtering.Filter;
import com.zendesk.maxwell.monitoring.Metrics;
import com.zendesk.maxwell.producer.AbstractProducer;
import com.zendesk.maxwell.row.HeartbeatRowMap;
import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.row.RowMapBuffer;
import com.zendesk.maxwell.schema.Schema;
import com.zendesk.maxwell.schema.SchemaStore;
import com.zendesk.maxwell.schema.SchemaStoreException;
import com.zendesk.maxwell.schema.Table;
import com.zendesk.maxwell.schema.ddl.DDLMap;
import com.zendesk.maxwell.schema.ddl.ResolvedSchemaChange;
import com.zendesk.maxwell.util.RunLoopProcess;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

public class BinlogConnectorReplicator extends RunLoopProcess implements Replicator {
	static final Logger LOGGER = LoggerFactory.getLogger(BinlogConnectorReplicator.class);
	private static final long MAX_TX_ELEMENTS = 10000;

	private final String clientID;
	private final String maxwellSchemaDatabaseName;

	private final BinaryLogClient client;
	private BinlogConnectorEventListener binlogEventListener;
	private BinlogConnectorLifecycleListener binlogLifecycleListener;
	private final LinkedBlockingDeque<BinlogConnectorEvent> queue = new LinkedBlockingDeque<>(20);
	private final TableCache tableCache = new TableCache();
	private final Scripting scripting;

	private final boolean stopOnEOF;
	private boolean hitEOF = false;

	private Position lastHeartbeatPosition;
	private final HeartbeatNotifier heartbeatNotifier;
	private Long stopAtHeartbeat;
	private Filter filter;

	private final AbstractBootstrapper bootstrapper;
	private final AbstractProducer producer;
	private RowMapBuffer rowBuffer;

	private final Counter rowCounter;
	private final Meter rowMeter;
	private SchemaStore schemaStore;
	private Histogram transactionRowCount;
	private Histogram transactionExecutionTime;


	private static Pattern createTablePattern =
		Pattern.compile("^CREATE\\s+TABLE", Pattern.CASE_INSENSITIVE);


	public BinlogConnectorReplicator(
		SchemaStore schemaStore,
		AbstractProducer producer,
		AbstractBootstrapper bootstrapper,
		MaxwellMysqlConfig mysqlConfig,
		Long replicaServerID,
		String maxwellSchemaDatabaseName,
		Metrics metrics,
		Position start,
		boolean stopOnEOF,
		String clientID,
		HeartbeatNotifier heartbeatNotifier,
		Scripting scripting
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

		/* setup metrics */
		rowCounter = metrics.getRegistry().counter(
			metrics.metricName("row", "count")
		);

		rowMeter = metrics.getRegistry().meter(
			metrics.metricName("row", "meter")
		);

		transactionRowCount = metrics.getRegistry().histogram(metrics.metricName("transaction", "row_count"));
		transactionExecutionTime = metrics.getRegistry().histogram(metrics.metricName("transaction", "execution_time"));

		/* setup binlog */
		this.binlogLifecycleListener = new BinlogConnectorLifecycleListener();
		this.client = new BinaryLogClient(mysqlConfig.host, mysqlConfig.port, mysqlConfig.user, mysqlConfig.password);

		this.client.setSSLMode(mysqlConfig.sslMode);

		BinlogPosition startBinlog = start.getBinlogPosition();
		if (startBinlog.getGtidSetStr() != null) {
			String gtidStr = startBinlog.getGtidSetStr();
			LOGGER.info("Setting initial gtid to: " + gtidStr);
			this.client.setGtidSet(gtidStr);
		} else {
			LOGGER.info("Setting initial binlog pos to: " + startBinlog.getFile() + ":" + startBinlog.getOffset());
			this.client.setBinlogFilename(startBinlog.getFile());
			this.client.setBinlogPosition(startBinlog.getOffset());
		}

		EventDeserializer eventDeserializer = new EventDeserializer();
		eventDeserializer.setCompatibilityMode(
			EventDeserializer.CompatibilityMode.DATE_AND_TIME_AS_LONG_MICRO,
			EventDeserializer.CompatibilityMode.CHAR_AND_BINARY_AS_BYTE_ARRAY,
			EventDeserializer.CompatibilityMode.INVALID_DATE_AND_TIME_AS_MIN_VALUE
		);
		this.client.setEventDeserializer(eventDeserializer);
		this.binlogEventListener = new BinlogConnectorEventListener(client, queue, metrics);
		this.client.setBlocking(!stopOnEOF);
		this.client.registerEventListener(binlogEventListener);
		this.client.registerLifecycleListener(binlogLifecycleListener);
		this.client.setServerId(replicaServerID.intValue());
	}

	/**
	 * get a single row from the replicator and pass it to the producer or bootstrapper.
	 *
	 * This is the top-level function in the run-loop.
	 */
	public void work() throws Exception {
		RowMap row = getRow();

		if ( row == null )
			return;

		rowCounter.inc();
		rowMeter.mark();

		if ( scripting != null )
			scripting.invoke(row);

		processRow(row);
	}

	public void startReplicator() throws Exception {
		this.client.connect(5000);
	}

	@Override
	protected void beforeStart() throws Exception {
		startReplicator();
	}

	@Override
	protected void beforeStop() throws Exception {
		this.binlogEventListener.stop();
		this.client.disconnect();
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
		} else if (!bootstrapper.shouldSkip(row) && !isMaxwellRow(row))
			producer.push(row);
		else
			bootstrapper.work(row, producer, this);
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
		LOGGER.debug("replicator picked up heartbeat: " + lastHeartbeatRead);
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
		for (ResolvedSchemaChange change : changes) {
			if (change.shouldOutput(filter)) {
				DDLMap ddl = new DDLMap(change, timestamp, sql, position, nextPosition);

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

	private boolean isSystemWhitelisted(String database, String table) {
		return this.maxwellSchemaDatabaseName.equals(database)
			&& ("bootstrap".equals(table) || "heartbeats".equals(table));
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
	 * Finall, if we decide to exclude a table we check the filter to
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
		else if ( isSystemWhitelisted(database, table) )
			return true;
		else {
			if ( Filter.includes(filter, database, table) )
				return true;
			else
				return Filter.couldIncludeFromColumnFilters(filter, database, table, columnNames);
		}
	}


	private boolean shouldOutputRowMap(String database, String table, RowMap rowMap, Filter filter) {
		return isSystemWhitelisted(database, table) ||
			Filter.includes(filter, database, table, rowMap.getData());
	}

	/**
	 * Is this RowMap an update to one of maxwell's own tables?
	 *
	 * If so we will often suppress the output.
	 * @param row The RowMap in question
	 * @return whether the update is something maxwell itself generated
	 */
	protected boolean isMaxwellRow(RowMap row) {
		return row.getDatabase().equals(this.maxwellSchemaDatabaseName);
	}

	private void ensureReplicatorThread() throws Exception {
		if ( !client.isConnected() && !stopOnEOF ) {
			String gtidStr = client.getGtidSet();
			String binlogPos = client.getBinlogFilename() + ":" + client.getBinlogPosition();
			String position = gtidStr == null ? binlogPos : gtidStr;
			LOGGER.warn("replicator stopped at position: " + position + " -- restarting");
			client.connect(5000);
		}
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
		RowMapBuffer buffer = new RowMapBuffer(MAX_TX_ELEMENTS);

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
						for ( RowMap r : event.jsonMaps(table, getLastHeartbeatRead(), currentQuery) )
							if (shouldOutputRowMap(table.getDatabase(), table.getName(), r, filter)) {
								buffer.add(r);
							}
					}
					currentQuery = null;
					break;
				case TABLE_MAP:
					TableMapEventData data = event.tableMapData();
					tableCache.processEvent(getSchema(), this.filter, data.getTableId(), data.getDatabase(), data.getTable());
					break;
				case ROWS_QUERY:
					RowsQueryEventData rqed = event.getEvent().getData();
					currentQuery = rqed.getQuery();
					break;
				case QUERY:
					QueryEventData qe = event.queryData();
					String sql = qe.getSql();

					if ( sql.toUpperCase().startsWith(BinlogConnectorEvent.SAVEPOINT)) {
						LOGGER.debug("Ignoring SAVEPOINT in transaction: " + qe);
					} else if ( createTablePattern.matcher(sql).find() ) {
						// CREATE TABLE `foo` SELECT * FROM `bar` will put a CREATE TABLE
						// inside a transaction.  Note that this could, in rare cases, lead
						// to us starting on a WRITE_ROWS event -- we sync the schema position somewhere
						// kinda unsafe.
						processQueryEvent(event);
					} else if (sql.toUpperCase().startsWith("INSERT INTO MYSQL.RDS_HEARTBEAT")) {
						// RDS heartbeat events take the following form:
						// INSERT INTO mysql.rds_heartbeat2(id, value) values (1,1483041015005) ON DUPLICATE KEY UPDATE value = 1483041015005
						// We don't need to process them, just ignore
					} else if (sql.toUpperCase().startsWith("DROP TEMPORARY TABLE")) {
						// Ignore temporary table drop statements inside transactions
					} else {
						LOGGER.warn("Unhandled QueryEvent inside transaction: " + qe);
					}
					break;
			}
		}
	}

	/**
	 * The main entry point into the event reading loop.
	 *
	 * We maintain a buffer of events in a transaction,
	 * and each subsequent call to `getRow` can grab one from
	 * the buffer.  If that buffer is empty, we'll go check
	 * the open-replicator buffer for rows to process.  If that
	 * buffer is empty, we return null.
	 *
	 * @return either a RowMap or null
	 */
	public RowMap getRow() throws Exception {
		BinlogConnectorEvent event;

		if ( stopOnEOF && hitEOF )
			return null;

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
					if ( client.isConnected() )
						continue;
					else
						return null;
				} else {
					ensureReplicatorThread();
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
					tableCache.processEvent(getSchema(), this.filter, data.getTableId(), data.getDatabase(), data.getTable());
					break;
				case QUERY:
					QueryEventData qe = event.queryData();
					String sql = qe.getSql();
					if (BinlogConnectorEvent.BEGIN.equals(sql)) {
						rowBuffer = getTransactionRows(event);
						rowBuffer.setServerId(event.getEvent().getHeader().getServerId());
						rowBuffer.setThreadId(qe.getThreadId());
					} else {
						processQueryEvent(event);
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

	public void setFilter(Filter filter) {
		this.filter = filter;
	}

	protected BinlogConnectorEvent pollEvent() throws InterruptedException {
		return queue.poll(100, TimeUnit.MILLISECONDS);
	}

	public Schema getSchema() throws SchemaStoreException {
		return this.schemaStore.getSchema();
	}


}
