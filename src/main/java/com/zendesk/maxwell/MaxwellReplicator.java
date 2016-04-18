package com.zendesk.maxwell;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import com.google.code.or.binlog.impl.event.*;
import com.google.code.or.net.TransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.code.or.OpenReplicator;
import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.common.util.MySQLConstants;
import com.zendesk.maxwell.bootstrap.AbstractBootstrapper;
import com.zendesk.maxwell.producer.AbstractProducer;
import com.zendesk.maxwell.schema.Schema;
import com.zendesk.maxwell.schema.SchemaStore;
import com.zendesk.maxwell.schema.Table;
import com.zendesk.maxwell.schema.ddl.SchemaChange;
import com.zendesk.maxwell.schema.ddl.ResolvedSchemaChange;
import com.zendesk.maxwell.schema.ddl.InvalidSchemaError;

public class MaxwellReplicator extends RunLoopProcess {
	private final long MAX_TX_ELEMENTS = 10000;
	String filePath, fileName;
	private long rowEventsProcessed;
	protected Schema schema;
	private MaxwellFilter filter;

	private final LinkedBlockingDeque<BinlogEventV4> queue = new LinkedBlockingDeque<>(20);

	protected MaxwellBinlogEventListener binlogEventListener;

	private final MaxwellTableCache tableCache = new MaxwellTableCache();
	protected final OpenReplicator replicator;
	private final MaxwellContext context;
	protected final AbstractProducer producer;
	protected final AbstractBootstrapper bootstrapper;

	static final Logger LOGGER = LoggerFactory.getLogger(MaxwellReplicator.class);

	public MaxwellReplicator(Schema currentSchema, AbstractProducer producer, AbstractBootstrapper bootstrapper, MaxwellContext ctx, BinlogPosition start) throws Exception {
		this.schema = currentSchema;

		this.binlogEventListener = new MaxwellBinlogEventListener(queue);

		this.replicator = new OpenReplicator();
		this.replicator.setBinlogEventListener(this.binlogEventListener);

		this.replicator.setHost(ctx.getConfig().replicationMysql.host);
		this.replicator.setUser(ctx.getConfig().replicationMysql.user);
		this.replicator.setPassword(ctx.getConfig().replicationMysql.password);
		this.replicator.setPort(ctx.getConfig().replicationMysql.port);

		this.replicator.setLevel2BufferSize(50 * 1024 * 1024);

		this.replicator.setHeartbeatPeriod(0.5f);

		this.producer = producer;
		this.bootstrapper = bootstrapper;

		this.context = ctx;
		this.setBinlogPosition(start);
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

		Long ms = replicator.millisSinceLastEvent();
		if ( ms != null && ms > 2000 ) {
			LOGGER.warn("no heartbeat heard from server in " + ms + "ms.  restarting replication.");
			replicator.stop(5, TimeUnit.SECONDS);
			replicator.start();
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

		context.ensurePositionThread();

		if (row == null)
			return;

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
		return row.getDatabase().equals(this.context.getConfig().databaseName);
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
				ew = new MaxwellWriteRowsEvent((WriteRowsEvent) e, table, filter);
				break;
			case MySQLConstants.WRITE_ROWS_EVENT_V2:
				ew = new MaxwellWriteRowsEvent((WriteRowsEventV2) e, table, filter);
				break;
			case MySQLConstants.UPDATE_ROWS_EVENT:
				ew = new MaxwellUpdateRowsEvent((UpdateRowsEvent) e, table, filter);
				break;
			case MySQLConstants.UPDATE_ROWS_EVENT_V2:
				ew = new MaxwellUpdateRowsEvent((UpdateRowsEventV2) e, table, filter);
				break;
			case MySQLConstants.DELETE_ROWS_EVENT:
				ew = new MaxwellDeleteRowsEvent((DeleteRowsEvent) e, table, filter);
				break;
			case MySQLConstants.DELETE_ROWS_EVENT_V2:
				ew = new MaxwellDeleteRowsEvent((DeleteRowsEventV2) e, table, filter);
				break;
			default:
				return null;
		}
		return ew;
	}

	private static Pattern createTablePattern =
			Pattern.compile("^CREATE\\s+TABLE", Pattern.CASE_INSENSITIVE);

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

			switch(v4Event.getHeader().getEventType()) {
				case MySQLConstants.WRITE_ROWS_EVENT:
				case MySQLConstants.WRITE_ROWS_EVENT_V2:
				case MySQLConstants.UPDATE_ROWS_EVENT:
				case MySQLConstants.UPDATE_ROWS_EVENT_V2:
				case MySQLConstants.DELETE_ROWS_EVENT:
				case MySQLConstants.DELETE_ROWS_EVENT_V2:
					rowEventsProcessed++;
					event = processRowsEvent((AbstractRowEvent) v4Event);

					if ( event == null ) {
						continue;
					}

					if ( event.matchesFilter() ) {
						for ( RowMap r : event.jsonMaps() )
							buffer.add(r);
					}

					setReplicatorPosition(event);

					break;
				case MySQLConstants.TABLE_MAP_EVENT:
					tableCache.processEvent(this.schema, this.filter, (TableMapEvent) v4Event);
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
					tableCache.processEvent(this.schema, this.filter, (TableMapEvent) v4Event);
					break;
				case MySQLConstants.QUERY_EVENT:
					QueryEvent qe = (QueryEvent) v4Event;
					if (qe.getSql().toString().equals("BEGIN"))
						rowBuffer = getTransactionRows();
					else
						processQueryEvent((QueryEvent) v4Event);
					break;
				default:
					break;
			}

			setReplicatorPosition((AbstractBinlogEventV4) v4Event);
		}
	}

	protected BinlogEventV4 pollV4EventFromQueue() throws InterruptedException {
		return queue.poll(100, TimeUnit.MILLISECONDS);
	}


	private void processQueryEvent(QueryEvent event) throws InvalidSchemaError, SQLException, IOException {
		// get charset of the alter event somehow? or just ignore it.
		String dbName = event.getDatabaseName().toString();
		String sql = event.getSql().toString();

		List<SchemaChange> changes = SchemaChange.parse(dbName, sql);

		if ( changes == null )
			return;

		Schema updatedSchema = this.schema;

		for ( SchemaChange change : changes ) {
			if ( !change.isBlacklisted(this.filter) ) {
				ResolvedSchemaChange resolved = change.resolve(updatedSchema);
				if ( resolved != null )
					updatedSchema = resolved.apply(updatedSchema);
			} else {
				LOGGER.debug("ignoring blacklisted schema change");
			}
		}

		if ( updatedSchema != this.schema) {
			BinlogPosition p = eventBinlogPosition(event);
			LOGGER.info("storing schema @" + p + " after applying \"" + sql.replace('\n', ' ') + "\"");

			saveSchema(updatedSchema, p);
		}
	}

	private void saveSchema(Schema updatedSchema, BinlogPosition p) throws SQLException {
		this.schema = updatedSchema;
		tableCache.clear();

		if ( !this.context.getReplayMode() ) {
			try (Connection c = this.context.getMaxwellConnection()) {
				new SchemaStore(c, this.context.getServerID(), this.schema, p, this.context.getConfig().databaseName).save();
			}

			this.context.setPositionSync(p);
		}
	}

	public Schema getSchema() {
		return schema;
	}

	public void setSchema(Schema schema) {
		this.schema = schema;
	}

	public void setFilter(MaxwellFilter filter) {
		this.filter = filter;
	}

	private void setReplicatorPosition(AbstractBinlogEventV4 e) {
		replicator.setBinlogFileName(e.getBinlogFilename());
		replicator.setBinlogPosition(e.getHeader().getNextPosition());
	}

	public OpenReplicator getOpenReplicator() {
		return replicator;
	}

}



