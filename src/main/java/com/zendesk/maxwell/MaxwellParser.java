package com.zendesk.maxwell;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.google.code.or.OpenReplicator;
import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.impl.FileBasedBinlogParser;
import com.google.code.or.binlog.impl.event.AbstractRowEvent;
import com.google.code.or.binlog.impl.event.QueryEvent;
import com.google.code.or.binlog.impl.event.TableMapEvent;
import com.google.code.or.binlog.impl.event.UpdateRowsEvent;
import com.google.code.or.binlog.impl.event.WriteRowsEvent;
import com.google.code.or.common.util.MySQLConstants;
import com.zendesk.maxwell.schema.Schema;
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

	public MaxwellParser(Schema currentSchema) throws Exception {
		this.schema = currentSchema;

		TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
		this.binlogEventListener = new MaxwellBinlogEventListener(queue);

		this.replicator = new OpenReplicator();

		this.replicator.setHost("127.0.0.1");
		this.replicator.setUser("maxwell");
		this.replicator.setPassword("maxwell");
		this.replicator.setBinlogEventListener(this.binlogEventListener);
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
			System.out.println(event.toJSON());
		}
	}

	private MaxwellAbstractRowsEvent processRowsEvent(AbstractRowEvent e) {
		MaxwellAbstractRowsEvent ew;
		Table table;

		table = tableCache.getTable(e.getTableId());

		if ( table == null ) {
			// TODO: richer error
			throw new RuntimeException("couldn't find table in cache for " + e.getTableId());
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

	private void processQueryEvent(QueryEvent event) throws SchemaSyncError {
		// get encoding of the alter event somehow; or just fuck it.
		String dbName = event.getDatabaseName().toString();
		String sql = event.getSql().toString();

		List<SchemaChange> changes = SchemaChange.parse(dbName, sql);
		for ( SchemaChange change : changes ) {
			this.schema = change.apply(this.schema);
		}

		if ( changes.size() > 0 ) {
			tableCache.clear();
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



