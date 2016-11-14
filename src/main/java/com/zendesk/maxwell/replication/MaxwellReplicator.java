package com.zendesk.maxwell.replication;

import java.sql.SQLException;
import java.util.Objects;
import java.util.List;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import com.google.code.or.binlog.impl.event.*;
import com.google.code.or.net.TransportException;
import com.zendesk.maxwell.*;
import com.zendesk.maxwell.row.HeartbeatRowMap;
import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.row.RowMapBuffer;
import com.zendesk.maxwell.schema.*;
import com.zendesk.maxwell.schema.ddl.DDLMap;
import com.zendesk.maxwell.schema.ddl.ResolvedSchemaChange;
import com.zendesk.maxwell.util.RunLoopProcess;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.code.or.OpenReplicator;
import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.common.util.MySQLConstants;
import com.zendesk.maxwell.bootstrap.AbstractBootstrapper;
import com.zendesk.maxwell.producer.AbstractProducer;

import com.zendesk.maxwell.schema.ddl.InvalidSchemaError;

public class MaxwellReplicator extends RunLoopProcess {
	private final long MAX_TX_ELEMENTS = 10000;
	protected SchemaStore schemaStore;

	private MaxwellFilter filter;
	private Long lastHeartbeatRead;

	private final LinkedBlockingDeque<BinlogEventV4> queue = new LinkedBlockingDeque<>(20);

	protected BinlogEventListener binlogEventListener;

	private final boolean shouldHeartbeat;
	private final TableCache tableCache = new TableCache();
	protected final OpenReplicator replicator;
	private final PositionStoreThread positionStoreThread;
	protected final AbstractProducer producer;
	protected final AbstractBootstrapper bootstrapper;
	private final String maxwellSchemaDatabaseName;
	private final String clientID;

	static final Logger LOGGER = LoggerFactory.getLogger(MaxwellReplicator.class);
	private final boolean stopOnEOF;
	private boolean hitEOF = false;

	public MaxwellReplicator(
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
		this.schemaStore = schemaStore;
		this.binlogEventListener = new BinlogEventListener(queue);

		this.replicator = new OpenReplicator();
		this.replicator.setBinlogEventListener(this.binlogEventListener);

		this.replicator.setHost(mysqlConfig.host);
		this.replicator.setUser(mysqlConfig.user);
		this.replicator.setPassword(mysqlConfig.password);
		this.replicator.setPort(mysqlConfig.port);
		this.replicator.setStopOnEOF(stopOnEOF);

		this.replicator.setLevel2BufferSize(50 * 1024 * 1024);
		this.replicator.setServerId(replicaServerID.intValue());

		this.shouldHeartbeat = shouldHeartbeat;
		if ( shouldHeartbeat )
			this.replicator.setHeartbeatPeriod(0.5f);

		this.producer = producer;
		this.bootstrapper = bootstrapper;
		this.stopOnEOF = stopOnEOF;

		this.positionStoreThread = positionStoreThread;
		this.maxwellSchemaDatabaseName = maxwellSchemaDatabaseName;
		this.setBinlogPosition(start);
		this.clientID = clientID;
	}

	public MaxwellReplicator(SchemaStore schemaStore, AbstractProducer producer, AbstractBootstrapper bootstrapper, MaxwellContext ctx, BinlogPosition start) throws SQLException {
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
		this.replicator.setBinlogFileName(p.getFile());
		this.replicator.setBinlogPosition(p.getOffset());
	}

	public void setPort(int port) {
		this.replicator.setPort(port);
	}

	private void ensureReplicatorThread() throws Exception {
		if ( !replicator.isRunning() && !replicator.isStopOnEOF() ) {
			LOGGER.warn("open-replicator stopped at position " + replicator.getBinlogFileName() + ":" + replicator.getBinlogPosition() + " -- restarting");
			replicator.start();
		}

		if ( shouldHeartbeat ) {
			Long ms = replicator.millisSinceLastEvent();
			if (ms != null && ms > 2000) {
				LOGGER.warn("no heartbeat heard from server in " + ms + "ms.  restarting replication.");
				replicator.stop(5, TimeUnit.SECONDS);
				replicator.start();
			}
		}
	}

	public void startReplicator() throws Exception {
		try {
			this.replicator.start();
		} catch ( TransportException e ) {
			switch ( e.getErrorCode() ) {
				case 1236:
					LOGGER.error("Missing binlog '" + this.replicator.getBinlogFileName() + "' on " + this.replicator.getHost());
				default:
					LOGGER.error("Transport exception #" + e.getErrorCode());
			}

			throw(e);
		}
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
		this.replicator.stop(5, TimeUnit.SECONDS);
	}

	protected boolean isMaxwellRow(RowMap row) {
		return row.getDatabase().equals(this.maxwellSchemaDatabaseName);
	}

	private BinlogPosition eventBinlogPosition(AbstractBinlogEventV4 event) {
		BinlogPosition p = new BinlogPosition(event.getHeader().getNextPosition(), event.getBinlogFilename());
		return p;
	}

	private AbstractRowsEvent processRowsEvent(AbstractRowEvent e) throws InvalidSchemaError {
		AbstractRowsEvent ew;
		Table table;

		long tableId = e.getTableId();

		if ( tableCache.isTableBlacklisted(tableId) ) {
			return null;
		}

		table = tableCache.getTable(tableId);

		if ( table == null ) {
			throw new InvalidSchemaError("couldn't find table in cache for table id: " + tableId);
		}

		switch (e.getHeader().getEventType()) {
			case MySQLConstants.WRITE_ROWS_EVENT:
				ew = new WriteRowsEvent((com.google.code.or.binlog.impl.event.WriteRowsEvent) e, table, filter, lastHeartbeatRead);
				break;
			case MySQLConstants.WRITE_ROWS_EVENT_V2:
				ew = new WriteRowsEvent((WriteRowsEventV2) e, table, filter, lastHeartbeatRead);
				break;
			case MySQLConstants.UPDATE_ROWS_EVENT:
				ew = new UpdateRowsEvent((com.google.code.or.binlog.impl.event.UpdateRowsEvent) e, table, filter, lastHeartbeatRead);
				break;
			case MySQLConstants.UPDATE_ROWS_EVENT_V2:
				ew = new UpdateRowsEvent((UpdateRowsEventV2) e, table, filter, lastHeartbeatRead);
				break;
			case MySQLConstants.DELETE_ROWS_EVENT:
				ew = new DeleteRowsEvent((com.google.code.or.binlog.impl.event.DeleteRowsEvent) e, table, filter, lastHeartbeatRead);
				break;
			case MySQLConstants.DELETE_ROWS_EVENT_V2:
				ew = new DeleteRowsEvent((DeleteRowsEventV2) e, table, filter, lastHeartbeatRead);
				break;
			default:
				return null;
		}
		return ew;
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
		BinlogEventV4 v4Event;
		AbstractRowsEvent event;

		RowMapBuffer buffer = new RowMapBuffer(MAX_TX_ELEMENTS);

		while ( true ) {
			v4Event = pollV4EventFromQueue();

			// currently to satisfy the test interface, the contract is to return null
			// if the queue is empty.  should probably just replace this with an optional timeout...
			if (v4Event == null) {
				ensureReplicatorThread();
				continue;
			}

			setReplicatorPosition((AbstractBinlogEventV4) v4Event);

			switch(v4Event.getHeader().getEventType()) {
				case MySQLConstants.WRITE_ROWS_EVENT:
				case MySQLConstants.WRITE_ROWS_EVENT_V2:
				case MySQLConstants.UPDATE_ROWS_EVENT:
				case MySQLConstants.UPDATE_ROWS_EVENT_V2:
				case MySQLConstants.DELETE_ROWS_EVENT:
				case MySQLConstants.DELETE_ROWS_EVENT_V2:
					event = processRowsEvent((AbstractRowEvent) v4Event);

					if ( event == null ) {
						continue;
					}

					String table = event.getTable().name;
					String database = event.getDatabase();

					Boolean isSystemWhitelisted = database.equals(this.maxwellSchemaDatabaseName)
							&& table.equals("bootstrap");

					/* there's an odd RDS thing, I guess, where ha_health_check doesn't
					 * show up in INFORMATION_SCHEMA but it's replicated nonetheless. */
					Boolean isSystemBlacklisted = database.equals("mysql") && table.equals("ha_health_check");

					if ( !isSystemBlacklisted && (event.matchesFilter() || isSystemWhitelisted) ) {
						for ( RowMap r : event.jsonMaps() )
							buffer.add(r);
					}

					break;
				case MySQLConstants.TABLE_MAP_EVENT:
					tableCache.processEvent(getSchema(), this.filter, (TableMapEvent) v4Event);
					break;
				case MySQLConstants.QUERY_EVENT:
					QueryEvent qe = (QueryEvent) v4Event;
					String sql = qe.getSql().toString();

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
						processQueryEvent(qe);
					} else {
						LOGGER.warn("Unhandled QueryEvent inside transaction: " + qe);
					}
					break;
				case MySQLConstants.XID_EVENT:
					XidEvent xe = (XidEvent) v4Event;

					buffer.setXid(xe.getXid());

					if ( !buffer.isEmpty() )
						buffer.getLast().setTXCommit();

					return buffer;
			}
		}
	}

	/**
	 * Get the last heartbeat that the replicator has processed.
	 *
	 * We pass along the value of the heartbeat to the producer inside the row map.
	 * @return the millisecond value ot fhte last heartbeat
	 */

	public Long getLastHeartbeatRead() {
		return lastHeartbeatRead;
	}


	/**
	 * Possibly convert a RowMap object into a HeartbeatRowMap
	 *
	 * Process a rowmap that represents a write to `maxwell`.`positions`.
	 * If it's a write for a different client_id, or it's not a heartbeat,
	 * we return just the RowMap.  Otherwise, we transform it into a HeartbeatRowMap
	 * and set lastHeartbeatRead.
	 *
	 * @return either a RowMap or a HeartbeatRowMap
	 */
	private RowMap processHeartbeats(RowMap row) throws SQLException {
		String hbClientID = (String) row.getData("client_id");
		if ( !Objects.equals(hbClientID, this.clientID) )
			return row;

		Object heartbeat_at = row.getData("heartbeat_at");
		Object old_heartbeat_at = row.getOldData("heartbeat_at"); // make sure it's a heartbeat update, not a position set.

		if ( heartbeat_at != null && old_heartbeat_at != null ) {
			Long thisHeartbeat = (Long) heartbeat_at;
			if ( !thisHeartbeat.equals(lastHeartbeatRead) ) {
				this.lastHeartbeatRead = thisHeartbeat;

				return HeartbeatRowMap.valueOf(row.getDatabase(), row.getPosition(), thisHeartbeat);
			}
		}
		return row;
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
		BinlogEventV4 v4Event;

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

			v4Event = pollV4EventFromQueue();

			if (v4Event == null) {
				if ( stopOnEOF ) {
					if ( replicator.isRunning() )
						continue;
					else
						return null;
				} else {
					ensureReplicatorThread();
					return null;
				}
			}

			switch (v4Event.getHeader().getEventType()) {
				case MySQLConstants.WRITE_ROWS_EVENT:
				case MySQLConstants.WRITE_ROWS_EVENT_V2:
				case MySQLConstants.UPDATE_ROWS_EVENT:
				case MySQLConstants.UPDATE_ROWS_EVENT_V2:
				case MySQLConstants.DELETE_ROWS_EVENT:
				case MySQLConstants.DELETE_ROWS_EVENT_V2:
					LOGGER.warn("Started replication stream outside of transaction.  This shouldn't normally happen.");

					queue.offerFirst(v4Event);
					rowBuffer = getTransactionRows();
					break;
				case MySQLConstants.TABLE_MAP_EVENT:
					tableCache.processEvent(getSchema(), this.filter, (TableMapEvent) v4Event);
					setReplicatorPosition((AbstractBinlogEventV4) v4Event);
					break;
				case MySQLConstants.QUERY_EVENT:
					QueryEvent qe = (QueryEvent) v4Event;
					if (qe.getSql().toString().equals("BEGIN")) {
						rowBuffer = getTransactionRows();
						rowBuffer.setServerId(qe.getHeader().getServerId());
						rowBuffer.setThreadId(qe.getThreadId());
					} else {
						processQueryEvent((QueryEvent) v4Event);
						setReplicatorPosition((AbstractBinlogEventV4) v4Event);
					}
					break;
				case MySQLConstants.ROTATE_EVENT:
					if ( stopOnEOF ) {
						this.replicator.stopQuietly(100, TimeUnit.MILLISECONDS);
						setReplicatorPosition((AbstractBinlogEventV4) v4Event);
						this.hitEOF = true;
						return null;
					}
					break;
				default:
					setReplicatorPosition((AbstractBinlogEventV4) v4Event);
					break;
			}

		}
	}

	protected BinlogEventV4 pollV4EventFromQueue() throws InterruptedException {
		return queue.poll(100, TimeUnit.MILLISECONDS);
	}

	private void processQueryEvent(QueryEvent event) throws SchemaStoreException, InvalidSchemaError, SQLException, Exception {
		// get charset of the alter event somehow? or just ignore it.
		String dbName = event.getDatabaseName().toString();
		String sql = event.getSql().toString();
		BinlogPosition position = eventBinlogPosition(event);

		List<ResolvedSchemaChange> changes =  schemaStore.processSQL(sql, dbName, position);
		for ( ResolvedSchemaChange change : changes ) {
			DDLMap ddl = new DDLMap(change,event.getHeader().getTimestamp(), sql, position);
			producer.push(ddl);
		}

		tableCache.clear();

		if ( this.producer != null )
			this.producer.writePosition(position);
	}

	public Schema getSchema() throws SchemaStoreException {
		return this.schemaStore.getSchema();
	}

	public void setFilter(MaxwellFilter filter) {
		this.filter = filter;
	}

	private void setReplicatorPosition(AbstractBinlogEventV4 e) {
		if ( e instanceof FormatDescriptionEvent ) // these have invalid positions
			return;

		replicator.setBinlogFileName(e.getBinlogFilename());
		replicator.setBinlogPosition(e.getHeader().getNextPosition());
	}

	public OpenReplicator getOpenReplicator() {
		return replicator;
	}

}
