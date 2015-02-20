package com.zendesk.maxwell;
import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.code.or.binlog.BinlogEventFilter;
import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.BinlogEventV4Header;
import com.google.code.or.binlog.BinlogParserContext;
import com.google.code.or.binlog.impl.FileBasedBinlogParser;
import com.google.code.or.binlog.impl.event.*;
import com.google.code.or.binlog.impl.parser.*;
import com.google.code.or.common.util.MySQLConstants;
import com.zendesk.maxwell.schema.Database;
import com.zendesk.maxwell.schema.Schema;
import com.zendesk.maxwell.schema.Table;
import com.zendesk.maxwell.schema.ddl.SchemaChange;
import com.zendesk.maxwell.schema.ddl.SchemaSyncError;

public class MaxwellParser {
	String filePath, fileName;
	private long rowEventsProcessed;
	private long startPosition;
	private Schema schema;
	private MaxwellFilter filter;

	private final LinkedBlockingQueue<BinlogEventV4> queue =  new LinkedBlockingQueue<BinlogEventV4>(20);

	protected FileBasedBinlogParser parser;
	protected MaxwellBinlogEventListener binlogEventListener;

	private final MaxwellTableCache tableCache = new MaxwellTableCache();

	public MaxwellParser(String filePath, String fileName, Schema currentSchema) throws Exception {
		this.filePath = filePath;
		this.fileName = fileName;
		this.startPosition = 4;
		this.schema = currentSchema;

		TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
		this.binlogEventListener = new MaxwellBinlogEventListener(queue);
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
			v4Event = getBinlogEvent();

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

	private BinlogEventV4 getBinlogEvent() throws Exception {
		BinlogEventV4 event;

		if (parser == null) {
			initParser(fileName, startPosition);
			parser.start();
		}

		while (true) {
			event = queue.poll(100, TimeUnit.MILLISECONDS);
			if (event != null) {
				if ( event instanceof RotateEvent ) {
					RotateEvent r = (RotateEvent) event;
					// we throw out the old parser and boot a new one.
					initParser(r.getBinlogFileName().toString(), r.getBinlogPosition());
					this.parser.start();
				} else {
					return event;
				}
			} else {
				if (!this.parser.isRunning()) {
					/* parser stopped and the queue is empty.
					   Likely that we've simply hit the end, but another possibility
					   is that the server crashed and never inserted the "rotate" event in
				       the logs.  Let's test for that. */

					String nextFile = findNextBinlogFile(this.fileName);
					if (nextFile == null)
						return null;
					else {
						initParser(nextFile, 4);
						this.parser.start();
					}
				}
			}
		}
	}

	private String findNextBinlogFile(String fileName) {
		Pattern p = Pattern.compile("(.*)\\.(\\d+)");
		Matcher m = p.matcher(fileName);
		if ( !m.matches() )
			return null;

		Integer nextInt = Integer.valueOf(m.group(2)) + 1;
		String testFile = m.group(1) + "." + String.format("%06d", nextInt);
		File f = new File(this.filePath + "/" + testFile);
		if ( f.exists() )
			return testFile;
		else
			return null;
	}

	protected FileBasedBinlogParser getDefaultBinlogParser() throws Exception {
		final FileBasedBinlogParser r = new FileBasedBinlogParser();
		r.registerEventParser(new StopEventParser());
		r.registerEventParser(new RotateEventParser());
		r.registerEventParser(new IntvarEventParser());
		r.registerEventParser(new XidEventParser());
		r.registerEventParser(new RandEventParser());
		r.registerEventParser(new QueryEventParser());
		r.registerEventParser(new UserVarEventParser());
		r.registerEventParser(new IncidentEventParser());
		r.registerEventParser(new TableMapEventParser());
		r.registerEventParser(new WriteRowsEventParser());
		r.registerEventParser(new UpdateRowsEventParser());
		r.registerEventParser(new DeleteRowsEventParser());
		r.registerEventParser(new WriteRowsEventV2Parser());
		r.registerEventParser(new UpdateRowsEventV2Parser());
		r.registerEventParser(new DeleteRowsEventV2Parser());
		r.registerEventParser(new FormatDescriptionEventParser());

		return r;
	}

	private void initParser(String fileName, long position) throws Exception {
		FileBasedBinlogParser bp = getDefaultBinlogParser();

		bp.setStartPosition(position);
		bp.setBinlogFileName(fileName);
		bp.setBinlogFilePath(filePath);

		bp.setEventListener(this.binlogEventListener);

		bp.setEventFilter(new BinlogEventFilter() {
			@Override
			public boolean accepts(BinlogEventV4Header header, BinlogParserContext context) {
				int eventType = header.getEventType();
				switch(eventType) {
				case MySQLConstants.WRITE_ROWS_EVENT:
				case MySQLConstants.WRITE_ROWS_EVENT_V2:
				case MySQLConstants.UPDATE_ROWS_EVENT:
				case MySQLConstants.UPDATE_ROWS_EVENT_V2:
				case MySQLConstants.DELETE_ROWS_EVENT:
				case MySQLConstants.DELETE_ROWS_EVENT_V2:
				case MySQLConstants.TABLE_MAP_EVENT:
				case MySQLConstants.ROTATE_EVENT:
				case MySQLConstants.QUERY_EVENT:
					return true;
				default:
					return false;
				}
			}
		});
		this.fileName = fileName;
		this.startPosition = position;
		this.parser = bp;
	}

	public void stop() {
		this.binlogEventListener.stop();
		try {
			this.parser.stop(200, TimeUnit.MILLISECONDS);
		} catch (Exception e) {
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

	public void setStartOffset(long pos) {
		this.startPosition = pos;
	}

	public void setFilter(MaxwellFilter filter) {
		this.filter = filter;
	}
}



