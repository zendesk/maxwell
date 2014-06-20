package com.zendesk.exodus;

import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.StringEscapeUtils;

import com.google.code.or.binlog.impl.event.AbstractRowEvent;
import com.google.code.or.binlog.impl.event.DeleteRowsEvent;
import com.google.code.or.binlog.impl.event.UpdateRowsEvent;
import com.google.code.or.binlog.impl.event.WriteRowsEvent;
import com.google.code.or.common.glossary.Column;
import com.google.code.or.common.glossary.Row;
import com.google.code.or.common.glossary.column.*;
import com.google.code.or.common.util.MySQLConstants;

public abstract class ExodusAbstractRowsEvent extends AbstractRowEvent {
	private static final SimpleDateFormat dateTimeFormatter = new SimpleDateFormat("''yyyy-MM-dd HH:mm:ss''");
	private static final SimpleDateFormat dateFormatter     = new SimpleDateFormat("''yyyy-MM-dd''");
	private AbstractRowEvent event;
	private String columnNames;
	protected String tableName;
	
	public ExodusAbstractRowsEvent(AbstractRowEvent e, String tableName, String columnNames) {
		this.tableId = e.getTableId();
		this.event = e;
		this.header = e.getHeader();
		this.tableName = tableName;
		this.columnNames = columnNames;
	}
	
	public static ExodusAbstractRowsEvent buildEvent(AbstractRowEvent e, String tableName, String columnNames, int idColumnOffset) {
		switch(e.getHeader().getEventType()) {
		case MySQLConstants.WRITE_ROWS_EVENT:
			return new ExodusWriteRowsEvent((WriteRowsEvent) e, tableName, columnNames);
		case MySQLConstants.UPDATE_ROWS_EVENT:
			return new ExodusUpdateRowsEvent((UpdateRowsEvent) e, tableName, columnNames);
		case MySQLConstants.DELETE_ROWS_EVENT:
			return new ExodusDeleteRowsEvent((DeleteRowsEvent) e, tableName, idColumnOffset);
		}
		return null;
	}
	public String toString() {
		return event.toString();
	}
	
	public abstract List<Row> getRows();
	public abstract String sqlOperationString();

	private String quoteString(String s) {
		return "'" + StringEscapeUtils.escapeSql(s) + "'";
	}
	private String columnToSql(Column c) {
		if ( c instanceof NullColumn ) {
			return "NULL";
		} else if ( c instanceof BlobColumn ||
				c instanceof StringColumn ) {
			byte[] b = (byte[]) c.getValue();
			String s = new String(b);
			return quoteString(s);
			
		} else if ( c instanceof DateColumn ||
				    c instanceof YearColumn ) {
			return dateFormatter.format(c.getValue());
		} else if ( c instanceof Datetime2Column ||
				    c instanceof DatetimeColumn ||
				    c instanceof Timestamp2Column ||
				    c instanceof TimestampColumn ) {
			return dateTimeFormatter.format(c.getValue());
		} else if ( c instanceof Int24Column || 
				c instanceof LongColumn || 
			    c instanceof LongLongColumn  ||
			    c instanceof ShortColumn ||
			    c instanceof TinyColumn ||
			    c instanceof DoubleColumn ||
			    c instanceof FloatColumn ) {
			return c.getValue().toString();
		} else {
			return null;	
		}
		
	}

	public String toSql() {
		StringBuilder sql = new StringBuilder();
		List<Row> rows = getRows();
		
		sql.append(sqlOperationString());
		sql.append(tableName);
		sql.append(columnNames);
		
		sql.append(" VALUES ");
		
		for(Iterator<Row> rowIter = rows.iterator(); rowIter.hasNext(); ) {
			Row row = rowIter.next();
			sql.append("\t(");
			for(Iterator<Column> iter = row.getColumns().iterator(); iter.hasNext(); ) { 
				Column c = iter.next();
				
				sql.append(columnToSql(c));
				
				if (iter.hasNext()) 
					sql.append(",");
			}
			if ( rowIter.hasNext() ) { 
				sql.append("),\n");	
			} else { 
				sql.append(")\n");
			}
		}
		
		return sql.toString();
	}
}
