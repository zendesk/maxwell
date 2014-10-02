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

import java.io.File;
import java.util.Date;
import java.util.HashMap;
import java.util.TimeZone;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

	private final LinkedBlockingQueue<BinlogEventV4> queue =  new LinkedBlockingQueue<BinlogEventV4>(20);

	protected FileBasedBinlogParser parser;
	protected ExodusBinlogEventListener binlogEventListener;

	public ExodusParser(String filePath, String fileName) throws Exception {
		this.filePath = filePath;
		this.fileName = fileName;
		this.startPosition = 4;

		TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
		this.binlogEventListener = new ExodusBinlogEventListener(queue);
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

	public void stop() {
		this.binlogEventListener.stop();
		try {
			this.parser.stop(200, TimeUnit.MILLISECONDS);
		} catch (Exception e) {
		}
	}

	public static void main(String args[]) throws Exception {
		int count = 0 ;
		BinlogEventV4 e;
		ExodusParser p = new ExodusParser("/opt/local/var/db/mysql5", "master.000001");
		Date start = new Date();

		while ((e = p.getEvent()) != null) {
			if ( e instanceof AbstractRowEvent ) {
				HashMap<String, Object> filter = new HashMap<String, Object>();
				ExodusColumnInfo columns[] = new ExodusColumnInfo[2];

				columns[0] = new ExodusColumnInfo("foo", "utf8", false);
				columns[1] = new ExodusColumnInfo("bar", "utf8", true);

				String encodings[] = new String[100];
				for (int i = 0 ; i < 100; i++) {
					encodings[i] = "utf8";
				}

				String s = ExodusAbstractRowsEvent.buildEvent((AbstractRowEvent) e, "nothing", columns, 1).toSql(filter);
				System.out.println(s);
			}

			count++;
			if ( count % 1000 == 0 ) {
				Date d = new Date();
				System.out.println("did " + Integer.toString(count) + " at " + (d.getTime() - start.getTime()) );
			}
		}
	}
}



