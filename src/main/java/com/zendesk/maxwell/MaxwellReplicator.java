package com.zendesk.maxwell;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.google.code.or.binlog.impl.event.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.code.or.OpenReplicator;
import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.common.util.MySQLConstants;
import com.zendesk.maxwell.producer.AbstractProducer;
import com.zendesk.maxwell.schema.Schema;
import com.zendesk.maxwell.schema.SchemaStore;
import com.zendesk.maxwell.schema.Table;
import com.zendesk.maxwell.schema.ddl.SchemaChange;
import com.zendesk.maxwell.schema.ddl.SchemaSyncError;

public class MaxwellReplicator extends RunLoopProcess {
	String filePath, fileName;
	private long rowEventsProcessed;
	private Schema schema;
	private MaxwellFilter filter;

	private final LinkedBlockingQueue<BinlogEventV4> queue = new LinkedBlockingQueue<BinlogEventV4>(20);

	protected MaxwellBinlogEventListener binlogEventListener;

	private final MaxwellTableCache tableCache = new MaxwellTableCache();
	protected final OpenReplicator replicator;
	private final MaxwellContext context;
	private final AbstractProducer producer;

	static final Logger LOGGER = LoggerFactory.getLogger(MaxwellReplicator.class);

	public MaxwellReplicator(Schema currentSchema, AbstractProducer producer, MaxwellContext ctx, BinlogPosition start) throws Exception {
		this.schema = currentSchema;

		TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
		this.binlogEventListener = new MaxwellBinlogEventListener(queue);

		this.replicator = new OpenReplicator();
		this.replicator.setBinlogEventListener(this.binlogEventListener);

		this.replicator.setHost(ctx.getConfig().mysqlHost);
		this.replicator.setUser(ctx.getConfig().mysqlUser);
		this.replicator.setPassword(ctx.getConfig().mysqlPassword);
		this.replicator.setPort(ctx.getConfig().mysqlPort);

		this.replicator.setLevel2BufferSize(50 * 1024 * 1024);

		this.producer = producer;

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
	}

	@Override
	protected void beforeStart() throws Exception {
		this.replicator.start();
	}

	public void work() throws Exception {
		MaxwellAbstractRowsEvent event;

		event = getEvent();

		context.ensurePositionThread();

		if (event == null)
			return;

		if (!skipEvent(event)) {
			producer.push(event);
		}
	}

	@Override
	protected void beforeStop() throws Exception {
		this.binlogEventListener.stop();
		this.replicator.stop(5, TimeUnit.SECONDS);
	}


	private boolean skipEvent(MaxwellAbstractRowsEvent event) {
		return event.getTable().getDatabase().getName().equals("maxwell");
	}

	private BinlogPosition eventBinlogPosition(AbstractBinlogEventV4 event) {
		BinlogPosition p = new BinlogPosition(event.getHeader().getNextPosition(), event.getBinlogFilename());
		return p;
	}

	private MaxwellAbstractRowsEvent processRowsEvent(AbstractRowEvent e) throws SchemaSyncError {
		MaxwellAbstractRowsEvent ew;
		Table table;

		table = tableCache.getTable(e.getTableId());

		if (table == null) {
			throw new SchemaSyncError("couldn't find table in cache for table id: " + e.getTableId());
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

	private LinkedList<MaxwellAbstractRowsEvent> getTransactionEvents() throws Exception {
		BinlogEventV4 v4Event;
		MaxwellAbstractRowsEvent event;

		LinkedList<MaxwellAbstractRowsEvent> list = new LinkedList<>();

		// currently to satisfy the test interface, the contract is to return null
		// if the queue is empty.  should probably just replace this with an optional timeout...

		while ( true ) {
			v4Event = pollV4EventFromQueue();
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

					if ( event.matchesFilter() )
						list.add(event);

					setReplicatorPosition(event);

					break;
				case MySQLConstants.TABLE_MAP_EVENT:
					tableCache.processEvent(this.schema, (TableMapEvent) v4Event);
					break;
				case MySQLConstants.QUERY_EVENT:
					QueryEvent qe = (QueryEvent) v4Event;
					String sql = qe.getSql().toString();

					if ( sql.equals("COMMIT") ) {
						// some storage engines(?) will output a "COMMIT" QUERY_EVENT instead of a XID_EVENT.
						// not sure exactly how to trigger this.
						if ( !list.isEmpty() )
							list.getLast().setTXCommit();

						return list;
					} else if ( sql.toUpperCase().startsWith("SAVEPOINT")) {
						LOGGER.info("Ignoring SAVEPOINT in transaction: " + qe);
					} else {
						LOGGER.warn("Unhandled QueryEvent inside transaction: " + qe);
					}

					break;
				case MySQLConstants.XID_EVENT:
					XidEvent xe = (XidEvent) v4Event;
					for ( MaxwellAbstractRowsEvent e : list )
						e.setXid(xe.getXid());

					if ( !list.isEmpty() )
						list.getLast().setTXCommit();

					return list;
			}
		}
	}

	private LinkedList<MaxwellAbstractRowsEvent> txBuffer;

	public MaxwellAbstractRowsEvent getEvent() throws Exception {
		BinlogEventV4 v4Event;
		MaxwellAbstractRowsEvent event;

		while (true) {
			if (txBuffer != null && !txBuffer.isEmpty()) {
				return txBuffer.removeFirst();
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
					LOGGER.error("Got an unexpected row-event: " + v4Event);
					break;
				case MySQLConstants.TABLE_MAP_EVENT:
					tableCache.processEvent(this.schema, (TableMapEvent) v4Event);
					break;
				case MySQLConstants.QUERY_EVENT:
					QueryEvent qe = (QueryEvent) v4Event;
					if (qe.getSql().toString().equals("BEGIN"))
						txBuffer = getTransactionEvents();
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


	private void processQueryEvent(QueryEvent event) throws SchemaSyncError, SQLException, IOException {
		// get encoding of the alter event somehow? or just ignore it.
		String dbName = event.getDatabaseName().toString();
		String sql = event.getSql().toString();

		List<SchemaChange> changes = SchemaChange.parse(dbName, sql);

		if ( changes == null )
			return;

		Schema updatedSchema = this.schema;

		for ( SchemaChange change : changes ) {
			updatedSchema = change.apply(updatedSchema);
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
			try (Connection c = this.context.getConnectionPool().getConnection()) {
				new SchemaStore(c, this.context.getServerID(), this.schema, p).save();
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

	public long getRowEventsProcessed() {
		return rowEventsProcessed;
	}

	public void resetRowEventsProcessed() {
		rowEventsProcessed = 0;
	}

	public String getFilePath() {
		return filePath;
	}

	public String getFileName() {
		return fileName;
	}

	public void setFilter(MaxwellFilter filter) {
		this.filter = filter;
	}

	private void setReplicatorPosition(AbstractBinlogEventV4 e) {
		replicator.setBinlogFileName(e.getBinlogFilename());
		replicator.setBinlogPosition(e.getHeader().getNextPosition());
	}
}



