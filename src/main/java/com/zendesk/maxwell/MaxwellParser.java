package com.zendesk.maxwell;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.google.code.or.OpenReplicator;
import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.impl.FileBasedBinlogParser;
import com.google.code.or.binlog.impl.event.AbstractBinlogEventV4;
import com.google.code.or.binlog.impl.event.AbstractRowEvent;
import com.google.code.or.binlog.impl.event.QueryEvent;
import com.google.code.or.binlog.impl.event.TableMapEvent;
import com.google.code.or.binlog.impl.event.UpdateRowsEvent;
import com.google.code.or.binlog.impl.event.WriteRowsEvent;
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

	private final LinkedBlockingQueue<BinlogEventV4> queue =  new LinkedBlockingQueue<BinlogEventV4>(20);

	protected FileBasedBinlogParser parser;
	protected MaxwellBinlogEventListener binlogEventListener;

	private final MaxwellTableCache tableCache = new MaxwellTableCache();
	private final OpenReplicator replicator;
	private MaxwellConfig config;
	private final AbstractProducer producer;

	public MaxwellParser(Schema currentSchema, AbstractProducer producer) throws Exception {
		this.schema = currentSchema;

		TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
		this.binlogEventListener = new MaxwellBinlogEventListener(queue);

		this.replicator = new OpenReplicator();

		this.replicator.setBinlogEventListener(this.binlogEventListener);
		this.producer = producer;
	}

	public void setBinlogPosition(BinlogPosition p) {
		this.replicator.setBinlogFileName(p.getFile());
		this.replicator.setBinlogPosition(p.getOffset());
	}

	public void setConfig(MaxwellConfig c) throws FileNotFoundException, IOException, SQLException {
		this.replicator.setHost(c.mysqlHost);
		this.replicator.setUser(c.mysqlUser);
		this.replicator.setPassword(c.mysqlPassword);
		this.replicator.setPort(c.mysqlPort);
		this.config = c;
		setBinlogPosition(c.getInitialPosition());
	}

	public void setPort(int port) {
		this.replicator.setPort(port);
	}

	public void start() throws Exception {
		this.replicator.start();
	}

	public void stop() throws Exception {
		this.binlogEventListener.stop();
		this.replicator.stop(5, TimeUnit.SECONDS);
	}


	public void run() throws Exception {
		MaxwellAbstractRowsEvent event;

		this.start();

		for(;;) {
			event = getEvent();
			if ( event == null )
				continue;

			if ( !skipEvent(event)) {
				producer.push(event);
				// TODO:  we need to tell the producer to only store a stop-event on table-maps
			}
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

		if ( table == null ) {
			throw new SchemaSyncError("couldn't find table in cache for table id: " + e.getTableId());
		}

		switch (e.getHeader().getEventType()) {
        case MySQLConstants.WRITE_ROWS_EVENT:
        	ew = new MaxwellWriteRowsEvent((WriteRowsEvent) e, table, filter);
        	break;
        case MySQLConstants.UPDATE_ROWS_EVENT:
        	ew = new MaxwellUpdateRowsEvent((UpdateRowsEvent) e, table, filter);
        	break;
        case MySQLConstants.DELETE_ROWS_EVENT:
        	ew = new MaxwellDeleteRowsEvent(e, table, filter);
        	break;
        default:
        	return null;
		}
		return ew;

	}

	public MaxwellAbstractRowsEvent getEvent(boolean stopAtNextTableMap) throws Exception {
        BinlogEventV4 v4Event;
        MaxwellAbstractRowsEvent event;
		while (true) {
			v4Event = queue.poll(100, TimeUnit.MILLISECONDS);

			if ( v4Event == null )
				return null;

			switch(v4Event.getHeader().getEventType()) {
			case MySQLConstants.WRITE_ROWS_EVENT:
			case MySQLConstants.UPDATE_ROWS_EVENT:
			case MySQLConstants.DELETE_ROWS_EVENT:
				rowEventsProcessed++;
				event = processRowsEvent((AbstractRowEvent) v4Event);
				if ( event.matchesFilter() )
					return event;
				break;
			case MySQLConstants.TABLE_MAP_EVENT:
				if ( stopAtNextTableMap)
					return null;

				tableCache.processEvent(this.schema, (TableMapEvent) v4Event);
				break;
			case MySQLConstants.QUERY_EVENT:
				processQueryEvent((QueryEvent) v4Event);
			}
		}
	}

	public MaxwellAbstractRowsEvent getEvent() throws Exception {
		return getEvent(false);
	}

	private void processQueryEvent(QueryEvent event) throws SchemaSyncError, SQLException, IOException {
		// get encoding of the alter event somehow; or just fuck it.
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
			new SchemaStore(this.config.getMasterConnection(), schema, p).save();
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



