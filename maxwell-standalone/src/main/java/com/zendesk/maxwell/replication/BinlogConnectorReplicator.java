package com.zendesk.maxwell.replication;

import com.codahale.metrics.Histogram;
import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.*;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import com.github.shyiko.mysql.binlog.network.SSLMode;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.MaxwellMysqlConfig;
import com.zendesk.maxwell.bootstrap.AbstractBootstrapper;
import com.zendesk.maxwell.monitoring.Metrics;
import com.zendesk.maxwell.producer.AbstractProducer;
import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.row.RowMapBuffer;
import com.zendesk.maxwell.schema.Schema;
import com.zendesk.maxwell.schema.SchemaStore;
import com.zendesk.maxwell.schema.SchemaStoreException;
import com.zendesk.maxwell.schema.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

public class BinlogConnectorReplicator extends AbstractReplicator implements Replicator {
	private final long MAX_TX_ELEMENTS = 10000;
	protected SchemaStore schemaStore;

	private final LinkedBlockingDeque<BinlogConnectorEvent> queue = new LinkedBlockingDeque<>(20);

	private BinlogConnectorEventListener binlogEventListener;
	private BinlogConnectorLifecycleListener binlogLifecycleListener;

	private final BinaryLogClient client;

	static final Logger LOGGER = LoggerFactory.getLogger(BinlogConnectorReplicator.class);
	private final boolean stopOnEOF;
	private boolean hitEOF = false;
	private Histogram transactionRowCount;
	private Histogram transactionExecutionTime;

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
		HeartbeatNotifier heartbeatNotifier
	) {
		super(clientID, bootstrapper, maxwellSchemaDatabaseName, producer, metrics, start, heartbeatNotifier);
		this.schemaStore = schemaStore;
		transactionExecutionTime = metrics.getRegistry().histogram(metrics.metricName("transaction", "execution_time"));
		transactionRowCount = metrics.getRegistry().histogram(metrics.metricName("transaction", "row_count"));

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
		this.binlogLifecycleListener = new BinlogConnectorLifecycleListener();

		this.client.setBlocking(!stopOnEOF);
		this.client.registerEventListener(binlogEventListener);
		this.client.registerLifecycleListener(binlogLifecycleListener);
		this.client.setServerId(replicaServerID.intValue());

		this.stopOnEOF = stopOnEOF;
	}

	public BinlogConnectorReplicator(SchemaStore schemaStore, AbstractProducer producer, AbstractBootstrapper bootstrapper, MaxwellContext ctx, Position start) throws SQLException {
		this(
			schemaStore,
			producer,
			bootstrapper,
			ctx.getConfig().replicationMysql,
			ctx.getConfig().replicaServerID,
			ctx.getConfig().databaseName,
			ctx.getMetrics(),
			start,
			false,
			ctx.getConfig().clientID,
			ctx.getHeartbeatNotifier()
		);
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

	private static Pattern createTablePattern =
			Pattern.compile("^CREATE\\s+TABLE", Pattern.CASE_INSENSITIVE);

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

					if ( table != null && shouldOutputEvent(table.getDatabase(), table.getName(), filter) ) {
						for ( RowMap r : event.jsonMaps(table, lastHeartbeatPosition, currentQuery) )
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

	private RowMapBuffer rowBuffer;

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

	protected BinlogConnectorEvent pollEvent() throws InterruptedException {
		return queue.poll(100, TimeUnit.MILLISECONDS);
	}

	private void processQueryEvent(BinlogConnectorEvent event) throws Exception {
		QueryEventData data = event.queryData();
		processQueryEvent(
			data.getDatabase(),
			data.getSql(),
			this.schemaStore,
			lastHeartbeatPosition.withBinlogPosition(event.getPosition()),
			event.getEvent().getHeader().getTimestamp()
		);
	}

	public Schema getSchema() throws SchemaStoreException {
		return this.schemaStore.getSchema();
	}
}
