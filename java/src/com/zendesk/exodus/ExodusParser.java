package com.zendesk.exodus;
import com.google.code.or.OpenParser;
import com.google.code.or.binlog.BinlogEventFilter;
import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.BinlogEventV4Header;
import com.google.code.or.binlog.BinlogParser;
import com.google.code.or.binlog.BinlogParserContext;
import com.google.code.or.binlog.impl.FileBasedBinlogParser;
import com.google.code.or.binlog.impl.event.AbstractRowEvent;
import com.google.code.or.binlog.impl.event.RotateEvent;
import com.google.code.or.binlog.impl.event.WriteRowsEvent;
import com.google.code.or.binlog.impl.parser.DeleteRowsEventParser;
import com.google.code.or.binlog.impl.parser.DeleteRowsEventV2Parser;
import com.google.code.or.binlog.impl.parser.FormatDescriptionEventParser;
import com.google.code.or.binlog.impl.parser.IncidentEventParser;
import com.google.code.or.binlog.impl.parser.IntvarEventParser;
import com.google.code.or.binlog.impl.parser.QueryEventParser;
import com.google.code.or.binlog.impl.parser.RandEventParser;
import com.google.code.or.binlog.impl.parser.RotateEventParser;
import com.google.code.or.binlog.impl.parser.StopEventParser;
import com.google.code.or.binlog.impl.parser.TableMapEventParser;
import com.google.code.or.binlog.impl.parser.UpdateRowsEventParser;
import com.google.code.or.binlog.impl.parser.UpdateRowsEventV2Parser;
import com.google.code.or.binlog.impl.parser.UserVarEventParser;
import com.google.code.or.binlog.impl.parser.WriteRowsEventParser;
import com.google.code.or.binlog.impl.parser.WriteRowsEventV2Parser;
import com.google.code.or.binlog.impl.parser.XidEventParser;
import com.google.code.or.common.util.MySQLConstants;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

class ExodusParserListener implements com.google.code.or.binlog.BinlogParserListener {
	protected AtomicBoolean running;
	public ExodusParserListener(AtomicBoolean running) {
		this.running = running;
	}

	public boolean isRunning() { 
		return this.running.get();
	}
	public void onStart(BinlogParser parser) { 
		System.out.println("parser for " + ((FileBasedBinlogParser) parser).getBinlogFileName() + " started.");
		this.running.set(true);
	}

	public void onStop(BinlogParser parser) { 
		System.out.println("parser for " +  ((FileBasedBinlogParser) parser).getBinlogFileName() + " stopped.");
		this.running.set(false);
	}

	public void onException(BinlogParser parser, Exception eception) {
	}


}
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
	
	private final AtomicBoolean running = new AtomicBoolean(true);
	private LinkedBlockingQueue<BinlogEventV4> queue =  new LinkedBlockingQueue<BinlogEventV4>(100);

	protected FileBasedBinlogParser parser;
	protected ExodusBinlogEventListener binlogEventListener;
	private ExodusParserListener parserListener;
	private Map<Integer, ExodusRowFilter> rowFilters = new HashMap<Integer, ExodusRowFilter>();
	

	public ExodusParser(String filePath, String fileName) throws Exception {
		this.filePath = filePath;
		this.fileName = fileName;
		this.startPosition = 4;
		
		this.binlogEventListener = new ExodusBinlogEventListener(queue);
		this.parserListener = new ExodusParserListener(this.running);
	}

	public void addRowFilter(ExodusRowFilter f) {
		this.rowFilters.put(f.getTableId(), f);
	}	
	
	public BinlogEventV4 getEvent() throws Exception {
		BinlogEventV4 event;

		if (parser == null) {
			initParser(fileName, startPosition);
		}

		if (!parser.isRunning()) {
			parser.start();
		}

		while (true) {
			event = queue.poll(100, TimeUnit.MILLISECONDS);
			if (event != null) { 
				if ( event instanceof RotateEvent ) {
					System.out.println("Got a rotate event.");
					RotateEvent r = (RotateEvent) event;
					initParser(r.getBinlogFileName().toString(), r.getBinlogPosition());
					this.parser.start();   
					continue;
				} else {
					return event;
				}
			}
			if (this.running.get() == false) { return null; }
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
				
		bp.addParserListener(this.parserListener);
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



