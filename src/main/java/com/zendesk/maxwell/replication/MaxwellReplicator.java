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

public class MaxwellReplicator extends AbstractReplicator implements Replicator {
	private final long MAX_TX_ELEMENTS = 10000;
	protected SchemaStore schemaStore;

	private MaxwellFilter filter;

	private final LinkedBlockingDeque<BinlogEventV4> queue = new LinkedBlockingDeque<>(20);

	protected BinlogEventListener binlogEventListener;

	private final boolean shouldHeartbeat;
	protected final OpenReplicator replicator;
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
		super(clientID, bootstrapper, positionStoreThread, maxwellSchemaDatabaseName, producer);
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

		this.stopOnEOF = stopOnEOF;

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

	@Override
	protected void beforeStop() throws Exception {
		this.binlogEventListener.stop();
		this.replicator.stop(5, TimeUnit.SECONDS);
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

					if ( shouldOutputEvent(event.getDatabase(), event.getTable().getName(), filter) ) {
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

	@Override
	public RowMap getRow() throws Exception {
		BinlogEventV4 v4Event;

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

	private void processQueryEvent(QueryEvent event) throws Exception {
		processQueryEvent(
			event.getDatabaseName().toString(),
			event.getSql().toString(),
			this.schemaStore,
			eventBinlogPosition(event),
			event.getHeader().getTimestamp()
		);
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
