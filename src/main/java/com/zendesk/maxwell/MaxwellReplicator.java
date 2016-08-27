package com.zendesk.maxwell;

import java.sql.SQLException;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import com.google.code.or.binlog.impl.event.*;
import com.google.code.or.net.TransportException;
import com.zendesk.maxwell.schema.*;
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

	protected MaxwellBinlogEventListener binlogEventListener;

	private final boolean shouldHeartbeat;
	private final MaxwellTableCache tableCache = new MaxwellTableCache();
	protected final OpenReplicator replicator;
	private final PositionStoreThread positionStoreThread;
	protected final AbstractProducer producer;
	protected final AbstractBootstrapper bootstrapper;
	private final String maxwellSchemaDatabaseName;

	static final Logger LOGGER = LoggerFactory.getLogger(MaxwellReplicator.class);

	public MaxwellReplicator(
		SchemaStore schemaStore,
		AbstractProducer producer,
		AbstractBootstrapper bootstrapper,
		MaxwellMysqlConfig mysqlConfig,
		Long replicaServerID,
		boolean shouldHeartbeat,
		PositionStoreThread positionStoreThread,
		String maxwellSchemaDatabaseName,
		BinlogPosition start
	) throws Exception {
		this.schemaStore = schemaStore;
		this.binlogEventListener = new MaxwellBinlogEventListener(queue);

		this.replicator = new OpenReplicator();
		this.replicator.setBinlogEventListener(this.binlogEventListener);

		this.replicator.setHost(mysqlConfig.host);
		this.replicator.setUser(mysqlConfig.user);
		this.replicator.setPassword(mysqlConfig.password);
		this.replicator.setPort(mysqlConfig.port);

		this.replicator.setLevel2BufferSize(50 * 1024 * 1024);
		this.replicator.setServerId(replicaServerID.intValue());

		this.shouldHeartbeat = shouldHeartbeat;
		if ( shouldHeartbeat )
			this.replicator.setHeartbeatPeriod(0.5f);

		this.producer = producer;
		this.bootstrapper = bootstrapper;

		this.positionStoreThread = positionStoreThread;
		this.maxwellSchemaDatabaseName = maxwellSchemaDatabaseName;
		this.setBinlogPosition(start);
	}

	public MaxwellReplicator(SchemaStore schemaStore, AbstractProducer producer, AbstractBootstrapper bootstrapper, MaxwellContext ctx, BinlogPosition start) throws Exception {
		this(
			schemaStore,
			producer,
			bootstrapper,
			ctx.getConfig().replicationMysql,
			ctx.getConfig().replicaServerID,
			ctx.shouldHeartbeat(),
			ctx.getPositionStoreThread(),
			ctx.getConfig().databaseName,
			start
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
		if ( !replicator.isRunning() ) {
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

	@Override
	protected void beforeStart() throws Exception {
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

	public void work() throws Exception {
		RowMap row = getRow();

		// todo: this is inelegant.  Ideally the outer code would just
		// call this and tell us to stop if the positionThread is dead.
		if ( positionStoreThread.getException() != null )
			throw positionStoreThread.getException();

		if (row == null)
			return;

		if ( isMaxwellRow(row) && row.getTable().equals("positions") ) {
			Object heartbeat_at = row.getData("heartbeat_at");
			if ( heartbeat_at != null ) {
				lastHeartbeatRead = (Long) heartbeat_at;
			}
		}

		if ( !bootstrapper.shouldSkip(row) && !isMaxwellRow(row) ) {
			producer.push(row);
		} else {
			bootstrapper.work(row, producer, this);
		}

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

	private MaxwellAbstractRowsEvent processRowsEvent(AbstractRowEvent e) throws InvalidSchemaError {
		MaxwellAbstractRowsEvent ew;
		Table table;

		long tableId = e.getTableId();

		if ( tableCache.isTableBlacklisted(tableId) ) {
			LOGGER.debug(String.format("ignoring row event for blacklisted table %s", tableCache.getBlacklistedTableName(tableId)));
			return null;
		}

		table = tableCache.getTable(tableId);

		if ( table == null ) {
			throw new InvalidSchemaError("couldn't find table in cache for table id: " + tableId);
		}

		switch (e.getHeader().getEventType()) {
			case MySQLConstants.WRITE_ROWS_EVENT:
				ew = new MaxwellWriteRowsEvent((WriteRowsEvent) e, table, filter, lastHeartbeatRead);
				break;
			case MySQLConstants.WRITE_ROWS_EVENT_V2:
				ew = new MaxwellWriteRowsEvent((WriteRowsEventV2) e, table, filter, lastHeartbeatRead);
				break;
			case MySQLConstants.UPDATE_ROWS_EVENT:
				ew = new MaxwellUpdateRowsEvent((UpdateRowsEvent) e, table, filter, lastHeartbeatRead);
				break;
			case MySQLConstants.UPDATE_ROWS_EVENT_V2:
				ew = new MaxwellUpdateRowsEvent((UpdateRowsEventV2) e, table, filter, lastHeartbeatRead);
				break;
			case MySQLConstants.DELETE_ROWS_EVENT:
				ew = new MaxwellDeleteRowsEvent((DeleteRowsEvent) e, table, filter, lastHeartbeatRead);
				break;
			case MySQLConstants.DELETE_ROWS_EVENT_V2:
				ew = new MaxwellDeleteRowsEvent((DeleteRowsEventV2) e, table, filter, lastHeartbeatRead);
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
	 * transaction-id (xid) to the COMMIT object, so we process the entire transaction
	 * to give them all that property.
	 *
	 * @return A RowMapBuffer of rows; either in-memory or on disk.
	 */

	private RowMapBuffer getTransactionRows() throws Exception {
		BinlogEventV4 v4Event;
		MaxwellAbstractRowsEvent event;

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

					if ( event.matchesFilter() ) {
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

	private RowMapBuffer rowBuffer;

	public RowMap getRow() throws Exception {
		BinlogEventV4 v4Event;

		while (true) {
			if (rowBuffer != null && !rowBuffer.isEmpty()) {
				return rowBuffer.removeFirst();
			}

			v4Event = pollV4EventFromQueue();

			if (v4Event == null) {
				ensureReplicatorThread();
				return null;
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
					if (qe.getSql().toString().equals("BEGIN"))
						rowBuffer = getTransactionRows();
					else {
						processQueryEvent((QueryEvent) v4Event);
						setReplicatorPosition((AbstractBinlogEventV4) v4Event);
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


	private void processQueryEvent(QueryEvent event) throws SchemaStoreException, InvalidSchemaError, SQLException {
		// get charset of the alter event somehow? or just ignore it.
		String dbName = event.getDatabaseName().toString();
		String sql = event.getSql().toString();
		BinlogPosition position = eventBinlogPosition(event);

		schemaStore.processSQL(sql, dbName, position);
		tableCache.clear();
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



