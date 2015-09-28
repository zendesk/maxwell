package com.zendesk.maxwell;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.code.or.binlog.impl.event.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.code.or.OpenReplicator;
import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.impl.FileBasedBinlogParser;
import com.google.code.or.common.util.MySQLConstants;
import com.zendesk.maxwell.producer.AbstractProducer;
import com.zendesk.maxwell.schema.Schema;
import com.zendesk.maxwell.schema.SchemaStore;
import com.zendesk.maxwell.schema.Table;
import com.zendesk.maxwell.schema.ddl.SchemaChange;
import com.zendesk.maxwell.schema.ddl.SchemaSyncError;

public class MaxwellParser {
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

	private enum RunState { STOPPED, RUNNING, REQUEST_STOP };
	private volatile RunState runState;

	static final Logger LOGGER = LoggerFactory.getLogger(MaxwellParser.class);

	public MaxwellParser(Schema currentSchema, AbstractProducer producer, MaxwellContext ctx, BinlogPosition start) throws Exception {
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

	public void start() throws Exception {
		this.replicator.start();
	}

	public void stop() throws TimeoutException {
		stop(5000);
	}

	public void stop(long timeoutMS) throws TimeoutException {
		// note: we use stderr in this function as it's LOGGER.err() oftentimes
		// won't flush in time, and we lose the messages.
		long left = 0;
		this.runState = RunState.REQUEST_STOP;

		for (left = timeoutMS; left > 0 && this.runState == RunState.REQUEST_STOP; left -= 100)
			try { Thread.sleep(100); } catch (InterruptedException e) {
		}

		if( this.runState != RunState.STOPPED )
			throw new TimeoutException("Maxwell's main parser thread didn't die after " + timeoutMS + "ms.");
	}

	private void doRun() throws Exception {
		MaxwellAbstractRowsEvent event;

		while ( this.runState == RunState.RUNNING ) {
			event = getEvent();

			if ( !replicator.isRunning() ) {
				LOGGER.warn("open-replicator stopped at position " + replicator.getBinlogFileName() + ":" + replicator.getBinlogPosition() + " -- restarting");
				replicator.start();
			}

			context.ensurePositionThread();

			if ( event == null )
				continue;

			if ( !skipEvent(event)) {
				producer.push(event);
			}

			replicator.setBinlogFileName(event.getBinlogFilename());
			replicator.setBinlogPosition(event.getHeader().getNextPosition());
		}

		try {
			this.binlogEventListener.stop();
			this.replicator.stop(5, TimeUnit.SECONDS);
		} catch ( Exception e ) {
			LOGGER.error("Got exception while shutting down replicator: " + e);
		}

		this.runState = RunState.STOPPED;
	}

	public void run() throws Exception {
		this.start();
		this.runState = RunState.RUNNING;

		try {
			doRun();
		} finally {
			this.runState = RunState.STOPPED;
		}
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
			if (v4Event == null)
				continue;

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

			if (v4Event == null) return null;

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
		}
	}

	protected BinlogEventV4 pollV4EventFromQueue() throws InterruptedException {
		return queue.poll(100, TimeUnit.MILLISECONDS);
	}


	private void processQueryEvent(QueryEvent event) throws SchemaSyncError, SQLException, IOException {
		// get encoding of the alter event somehow? or just fuck it.
		String dbName = event.getDatabaseName().toString();
		String sql = event.getSql().toString();

		List<SchemaChange> changes = SchemaChange.parse(dbName, sql);

		if ( changes == null )
			return;

		for ( SchemaChange change : changes ) {
			this.schema = change.apply(this.schema);
		}

		if ( changes.size() > 0 ) {
			tableCache.clear();
			BinlogPosition p = eventBinlogPosition(event);
			LOGGER.info("storing schema @" + p + " after applying \"" + sql.replace('\n',' ') + "\"");
			try ( Connection c = this.context.getConnectionPool().getConnection() ) {
				new SchemaStore(c, this.context.getServerID(), schema, p).save();
			}
			this.context.setInitialPositionSync(p);
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

}



