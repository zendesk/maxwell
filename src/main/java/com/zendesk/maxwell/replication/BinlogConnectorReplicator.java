package com.zendesk.maxwell.replication;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.*;
import com.github.shyiko.mysql.binlog.event.deserialization.AbstractRowsEventDataDeserializer;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.MaxwellFilter;
import com.zendesk.maxwell.MaxwellMysqlConfig;
import com.zendesk.maxwell.bootstrap.AbstractBootstrapper;
import com.zendesk.maxwell.producer.AbstractProducer;
import com.zendesk.maxwell.row.HeartbeatRowMap;
import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.row.RowMapBuffer;
import com.zendesk.maxwell.schema.*;
import com.zendesk.maxwell.schema.ddl.DDLMap;
import com.zendesk.maxwell.schema.ddl.InvalidSchemaError;
import com.zendesk.maxwell.schema.ddl.ResolvedSchemaChange;
import com.zendesk.maxwell.util.RunLoopProcess;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

public class BinlogConnectorReplicator extends AbstractReplicator implements Replicator {
	private final long MAX_TX_ELEMENTS = 10000;
	protected SchemaStore schemaStore;

	private MaxwellFilter filter;

	private final LinkedBlockingDeque<BinlogConnectorEvent> queue = new LinkedBlockingDeque<>(20);

	protected BinlogConnectorEventListener binlogEventListener;

	// private final boolean shouldHeartbeat;
	private final TableCache tableCache = new TableCache();
	private final PositionStoreThread positionStoreThread;
	protected final AbstractProducer producer;
	protected final AbstractBootstrapper bootstrapper;
	private final String maxwellSchemaDatabaseName;

	private final BinaryLogClient client;

	static final Logger LOGGER = LoggerFactory.getLogger(MaxwellReplicator.class);
	private final boolean stopOnEOF;
	private boolean hitEOF = false;

	public BinlogConnectorReplicator(
		SchemaStore schemaStore,
		AbstractProducer producer,
		AbstractBootstrapper bootstrapper,
		MaxwellMysqlConfig mysqlConfig,
		Long replicaServerID,
		boolean shouldHeartbeat,
		PositionStoreThread positionStoreThread,
		String maxwellSchemaDatabaseName,
		BinlogPosition start,
		boolean stopOnEOF,
		String clientID
	) {
		super(clientID);
		this.schemaStore = schemaStore;

		this.client = new BinaryLogClient(mysqlConfig.host, mysqlConfig.port, mysqlConfig.user, mysqlConfig.password);

		EventDeserializer eventDeserializer = new EventDeserializer();
		eventDeserializer.setCompatibilityMode(EventDeserializer.CompatibilityMode.DATE_AND_TIME_AS_LONG,
			EventDeserializer.CompatibilityMode.CHAR_AND_BINARY_AS_BYTE_ARRAY);
		this.client.setEventDeserializer(eventDeserializer);

		this.binlogEventListener = new BinlogConnectorEventListener(client, queue);
		this.client.setBlocking(!stopOnEOF);
		this.client.registerEventListener(binlogEventListener);
		this.client.setServerId(replicaServerID.intValue());
		this.client.setBinlogFilename(start.getFile());
		this.client.setBinlogPosition(start.getOffset());

		/*
		this.shouldHeartbeat = shouldHeartbeat;
		if ( shouldHeartbeat )
			this.replicator.setHeartbeatPeriod(0.5f);
			*/

		this.producer = producer;
		this.bootstrapper = bootstrapper;
		this.stopOnEOF = stopOnEOF;

		this.positionStoreThread = positionStoreThread;
		this.maxwellSchemaDatabaseName = maxwellSchemaDatabaseName;
		this.setBinlogPosition(start);
	}

	public BinlogConnectorReplicator(SchemaStore schemaStore, AbstractProducer producer, AbstractBootstrapper bootstrapper, MaxwellContext ctx, BinlogPosition start) throws SQLException {
		this(
			schemaStore,
			producer,
			bootstrapper,
			ctx.getConfig().replicationMysql,
			ctx.getConfig().replicaServerID,
			ctx.shouldHeartbeat(),
			ctx.getPositionStoreThread(),
			ctx.getConfig().databaseName,
			start,
			false,
			ctx.getConfig().clientID
		);
	}

	public void setBinlogPosition(BinlogPosition p) {
		this.client.setBinlogFilename(p.getFile());
		this.client.setBinlogPosition(p.getOffset());
	}

	private void ensureReplicatorThread() throws Exception {
		if ( !client.isConnected() && !stopOnEOF ) {
			LOGGER.warn("replicator stopped at position " + client.getBinlogFilename() + ":" + client.getBinlogPosition() + " -- restarting");
			client.connect();
		}
	}

	public void startReplicator() throws Exception {
		this.client.connect(5000);
	}

	@Override
	protected void beforeStart() throws Exception {
		startReplicator();
	}

	public void work() throws Exception {
		RowMap row = getRow();

		// todo: this is inelegant.  Ideally the outer code would monitor the
		// position thread and stop us if it was dead.

		if ( positionStoreThread.getException() != null )
			throw positionStoreThread.getException();

		if ( row == null )
			return;

		if ( row instanceof HeartbeatRowMap)
			producer.push(row);
		else if (!bootstrapper.shouldSkip(row) && !isMaxwellRow(row))
			producer.push(row);
		else
			bootstrapper.work(row, producer, this);
	}

	@Override
	protected void beforeStop() throws Exception {
		this.binlogEventListener.stop();
		this.client.disconnect();
	}

	protected boolean isMaxwellRow(RowMap row) {
		return row.getDatabase().equals(this.maxwellSchemaDatabaseName);
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

	private RowMapBuffer getTransactionRows() throws Exception {
		BinlogConnectorEvent event;
		RowMapBuffer buffer = new RowMapBuffer(MAX_TX_ELEMENTS);

		while ( true ) {
			event = pollEvent();

			if (event == null) {
				ensureReplicatorThread();
				continue;
			}

			switch(event.getEvent().getHeader().getEventType()) {
				case WRITE_ROWS:
				case UPDATE_ROWS:
				case DELETE_ROWS:
				case EXT_WRITE_ROWS:
				case EXT_UPDATE_ROWS:
				case EXT_DELETE_ROWS:
					Table table = tableCache.getTable(event.getTableID());
					if ( table != null && (filter == null || filter.matches(table.getDatabase(), table.getName())) ) {
						for ( RowMap r : event.jsonMaps(table) )
							buffer.add(r);
					}

					break;
				case TABLE_MAP:
					TableMapEventData data = event.tableMapData();
					tableCache.processEvent(getSchema(), this.filter, data.getTableId(), data.getDatabase(), data.getTable());
					break;
				case QUERY:
					QueryEventData qe = event.queryData();
					String sql = qe.getSql();

					if ( sql.equals("COMMIT") ) {
						// MyISAM will output a "COMMIT" QUERY_EVENT instead of a XID_EVENT.
						// There's no transaction ID but we can still set "commit: true"
						if ( !buffer.isEmpty() )
							buffer.getLast().setTXCommit();

						return buffer;
					} else if ( sql.toUpperCase().startsWith("SAVEPOINT")) {
						LOGGER.info("Ignoring SAVEPOINT in transaction: " + qe);
					} else if ( createTablePattern.matcher(sql).find() ) {
						// CREATE TABLE `foo` SELECT * FROM `bar` will put a CREATE TABLE
						// inside a transaction.  Note that this could, in rare cases, lead
						// to us starting on a WRITE_ROWS event -- we sync the schema position somewhere
						// kinda unsafe.
						processQueryEvent(event);
					} else {
						LOGGER.warn("Unhandled QueryEvent inside transaction: " + qe);
					}
					break;
				case XID:
					buffer.setXid(event.xidData().getXid());

					if ( !buffer.isEmpty() )
						buffer.getLast().setTXCommit();

					return buffer;
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

				if ( row != null && isMaxwellRow(row) && row.getTable().equals("positions") )
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
					LOGGER.warn("Started replication stream outside of transaction.  This shouldn't normally happen.");

					queue.offerFirst(event);
					rowBuffer = getTransactionRows();
					break;
				case TABLE_MAP:
					TableMapEventData data = event.tableMapData();
					tableCache.processEvent(getSchema(), this.filter, data.getTableId(), data.getDatabase(), data.getTable());
					break;
				case QUERY:
					QueryEventData qe = event.queryData();
					String sql = qe.getSql();
					if (sql.equals("BEGIN")) {
						rowBuffer = getTransactionRows();
						rowBuffer.setServerId(event.getEvent().getHeader().getServerId());
						rowBuffer.setThreadId(qe.getThreadId());
					} else {
						processQueryEvent(event);
					}
					break;
				case ROTATE:
					if ( stopOnEOF ) {
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

		// get charset of the alter event somehow? or just ignore it.
		String dbName = data.getDatabase();
		String sql = data.getSql();

		List<ResolvedSchemaChange> changes =  schemaStore.processSQL(sql, dbName, event.getPosition());
		for ( ResolvedSchemaChange change : changes ) {
			DDLMap ddl = new DDLMap(change,event.getEvent().getHeader().getTimestamp(), sql, event.getPosition());
			producer.push(ddl);
		}

		tableCache.clear();

		if ( this.producer != null )
			this.producer.writePosition(event.getPosition());
	}

	public Schema getSchema() throws SchemaStoreException {
		return this.schemaStore.getSchema();
	}

	public void setFilter(MaxwellFilter filter) {
		this.filter = filter;
	}
}
