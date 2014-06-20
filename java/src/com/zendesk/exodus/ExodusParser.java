package com.zendesk.exodus;
import com.google.code.or.binlog.BinlogEventFilter;
import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.BinlogEventV4Header;
import com.google.code.or.binlog.BinlogParserContext;
import com.google.code.or.binlog.impl.FileBasedBinlogParser;
import com.google.code.or.binlog.impl.event.AbstractRowEvent;
import com.google.code.or.binlog.impl.event.RotateEvent;
import com.google.code.or.binlog.impl.parser.*;
import com.google.code.or.common.util.MySQLConstants;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;


public class ExodusParser {
	
	String filePath, fileName;
	private long startPosition;
	
	public String getFilePath() {
		return filePath;
	}

	public String getFileName() {
		return fileName;
	}

	public void setStartPosition(long pos) {
		this.startPosition = pos;
	}
	
	private LinkedBlockingQueue<BinlogEventV4> queue =  new LinkedBlockingQueue<BinlogEventV4>(100);

	protected FileBasedBinlogParser parser;
	protected ExodusBinlogEventListener binlogEventListener;
	private Map<Integer, ExodusRowFilter> rowFilters = new HashMap<Integer, ExodusRowFilter>();
	

	public ExodusParser(String filePath, String fileName) throws Exception {
		this.filePath = filePath;
		this.fileName = fileName;
		this.startPosition = 4;
		
		this.binlogEventListener = new ExodusBinlogEventListener(queue);
	}

	public void addRowFilter(ExodusRowFilter f) {
		this.rowFilters.put(f.getTableId(), f);
	}	
	
	public BinlogEventV4 getEvent() throws Exception {
		BinlogEventV4 event;

		if (parser == null) {
			initParser(fileName, startPosition);
			parser.start();
		}

		while (true) {
			event = queue.poll(100, TimeUnit.MILLISECONDS);
			if (event != null) { 
				if ( event instanceof RotateEvent ) {
					// we throw out the old parser and let it stop.  It will (erroneously) stop us.  
					// I think there's a small race surface still, if we get this rotate event, and 
					// then the old parser calls stop, we could go down. 
					System.out.println("Got a rotate event.");
					RotateEvent r = (RotateEvent) event;
					initParser(r.getBinlogFileName().toString(), r.getBinlogPosition());
					this.parser.start();   
					continue;
				} else {
					return event;
				}
			}
			if (!this.parser.isRunning()) { return null; }
		}
	}

	protected FileBasedBinlogParser getDefaultBinlogParser() throws Exception {
		final FileBasedBinlogParser r = new FileBasedBinlogParser();
		r.registgerEventParser(new StopEventParser());
		r.registgerEventParser(new RotateEventParser());
		r.registgerEventParser(new IntvarEventParser());
		r.registgerEventParser(new XidEventParser());
		r.registgerEventParser(new RandEventParser());
		r.registgerEventParser(new QueryEventParser());
		r.registgerEventParser(new UserVarEventParser());
		r.registgerEventParser(new IncidentEventParser());
		r.registgerEventParser(new TableMapEventParser());
		r.registgerEventParser(new WriteRowsEventParser());
		r.registgerEventParser(new UpdateRowsEventParser());
		r.registgerEventParser(new DeleteRowsEventParser());
		r.registgerEventParser(new WriteRowsEventV2Parser());
		r.registgerEventParser(new UpdateRowsEventV2Parser());
		r.registgerEventParser(new DeleteRowsEventV2Parser());
		r.registgerEventParser(new FormatDescriptionEventParser());

		return r;
	}

	private void initParser(String fileName, long position) throws Exception {
		FileBasedBinlogParser bp = getDefaultBinlogParser();
		
		bp.setStartPosition(position);
		bp.setBinlogFileName(fileName);
		bp.setBinlogFilePath(filePath);
		
		bp.setEventListener(this.binlogEventListener);
				
		bp.setEventFilter(new BinlogEventFilter() { 
			public boolean accepts(BinlogEventV4Header header, BinlogParserContext context) { 
				int eventType = header.getEventType();
				return eventType == MySQLConstants.WRITE_ROWS_EVENT || 
						eventType == MySQLConstants.WRITE_ROWS_EVENT_V2 ||
						eventType == MySQLConstants.UPDATE_ROWS_EVENT ||
						eventType == MySQLConstants.UPDATE_ROWS_EVENT_V2 ||
						eventType == MySQLConstants.DELETE_ROWS_EVENT ||
						eventType == MySQLConstants.DELETE_ROWS_EVENT_V2 ||
						eventType == MySQLConstants.TABLE_MAP_EVENT || 
						eventType == MySQLConstants.ROTATE_EVENT;
			}
		}); 
		this.fileName = fileName;
		this.startPosition = position;
		this.parser = bp;
	}

	public static void main(String args[]) throws Exception {
		int count = 0 ;
		BinlogEventV4 e;
		ExodusParser p = new ExodusParser("/opt/local/var/db/mysql5", "master.000007");
		Date start = new Date();
		ExodusRowFilter f = new ExodusRowFilter(1, 3, 1);
		p.addRowFilter(f);
		while ((e = p.getEvent()) != null) {
			if ( e instanceof AbstractRowEvent ) {
				String s = ExodusAbstractRowsEvent.buildEvent((AbstractRowEvent) e, "nothing", "(foo,foo)", 1).toSql();
				System.out.println(s);
			}
			
			count++;
			if ( count % 1000 == 0 ) {
				Date d = new Date();
				System.out.println("did " + Integer.toString(count) + " at " + (d.getTime() - start.getTime()) );
			}
			
			//System.out.println(e.toString());
		}
	}


}



