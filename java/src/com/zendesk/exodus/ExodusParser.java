package com.zendesk.exodus;
import com.google.code.or.OpenParser;
import com.google.code.or.binlog.BinlogEventFilter;
import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.BinlogEventV4Header;
import com.google.code.or.binlog.BinlogParser;
import com.google.code.or.binlog.BinlogParserContext;
import com.google.code.or.binlog.impl.FileBasedBinlogParser;
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
	public void onStart(BinlogParser parser) { 	}

	public void onStop(BinlogParser parser) { 
		System.out.println("stopping parser");
		this.running.set(false);
	}

	public void onException(BinlogParser parser, Exception eception) {	}
	
	
}
public class ExodusParser {
	private String filePath, fileName;
	private final AtomicBoolean running = new AtomicBoolean(true);
	private LinkedBlockingQueue<BinlogEventV4> queue =  new LinkedBlockingQueue<BinlogEventV4>(100);
	
	protected final OpenParser op = new OpenParser();
	
	public ExodusParser(String filePath, String fileName) throws Exception {
	  this.filePath = filePath;
	  this.fileName = fileName;
	  
	  op.setStartPosition(4); // a default
	  op.setBinlogFileName(fileName);
	  op.setBinlogFilePath(filePath);
	  
	  BinlogParser bp = this.getDefaultBinlogParser();
	  bp.addParserListener(new ExodusParserListener(this.running));
	  bp.setEventFilter(new BinlogEventFilter() { 
		  public boolean accepts(BinlogEventV4Header header, BinlogParserContext context) { 
			  int eventType = header.getEventType();
			  return eventType == MySQLConstants.WRITE_ROWS_EVENT || 
					  eventType == MySQLConstants.WRITE_ROWS_EVENT_V2 ||
					  eventType == MySQLConstants.UPDATE_ROWS_EVENT ||
					  eventType == MySQLConstants.UPDATE_ROWS_EVENT_V2 ||
					  eventType == MySQLConstants.DELETE_ROWS_EVENT ||
					  eventType == MySQLConstants.DELETE_ROWS_EVENT_V2 ||
					  eventType == MySQLConstants.TABLE_MAP_EVENT;
		  }
	  });

	  op.setBinlogParser(bp);
	  op.setBinlogEventListener(new ExodusBinlogEventListener(queue));
	}
	
	public void setBinlogPosition(long pos) {
		op.setStartPosition(pos);
	}
	
	public BinlogEventV4 getEvent() throws Exception {
		BinlogEventV4 event;
		
		if (!op.isRunning()) {
			op.start();
		}
		
		while (true) {
			event = queue.poll(1, TimeUnit.SECONDS);
			if (event != null) { return event; }
			if (this.running.get() == false) { return null; }
		}
	}
	public static void main(String args[]) throws Exception {
		BinlogEventV4 e;
		ExodusParser p = new ExodusParser("/opt/local/var/db/mysql5", "master.000004");
		while ((e = p.getEvent()) != null) {
		  	// System.out.println(e.toString());
		}
	}
	
	protected FileBasedBinlogParser getDefaultBinlogParser() throws Exception {
		//
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
		
		//
		r.setStartPosition(op.getStartPosition());
		r.setBinlogFileName(this.fileName);
		r.setBinlogFilePath(this.filePath);
		return r;
	}
}



