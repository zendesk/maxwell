package com.zendesk.exodus;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.codec.binary.Hex;

import com.google.code.or.binlog.BinlogEventV4Header;
import com.google.code.or.binlog.impl.event.AbstractRowEvent;
import com.google.code.or.binlog.impl.event.DeleteRowsEvent;
import com.google.code.or.binlog.impl.event.UpdateRowsEvent;
import com.google.code.or.binlog.impl.event.WriteRowsEvent;
import com.google.code.or.common.glossary.Column;
import com.google.code.or.common.glossary.Row;
import com.google.code.or.common.glossary.column.*;
import com.google.code.or.common.util.MySQLConstants;

public abstract class ExodusAbstractRowsEvent extends AbstractRowEvent {
	private static final TimeZone tz = TimeZone.getTimeZone("UTC");

	private static SimpleDateFormat dateTimeFormatter;
	private static SimpleDateFormat dateFormatter;

	protected static SimpleDateFormat getDateTimeFormatter() {
		if ( dateTimeFormatter == null ) {
			dateTimeFormatter = new SimpleDateFormat("''yyyy-MM-dd HH:mm:ss''");
			dateTimeFormatter.setTimeZone(tz);
		}
		return dateTimeFormatter;
	}

	protected static SimpleDateFormat getDateFormatter() {
		if ( dateFormatter == null ) {
			dateFormatter = new SimpleDateFormat("''yyyy-MM-dd''");
			dateFormatter.setTimeZone(tz);
		}
		return dateFormatter;
	}


	private final AbstractRowEvent event;
	protected String tableName;

	private final ExodusColumnInfo[] columns;

	public ExodusAbstractRowsEvent(AbstractRowEvent e, String tableName, ExodusColumnInfo[] columns) {
		this.tableId = e.getTableId();
		this.event = e;
		this.header = e.getHeader();
		this.tableName = tableName;
		this.columns = columns;
	}

	@Override
	public BinlogEventV4Header getHeader() {
		return event.getHeader();
	}

	@Override
	public String getBinlogFilename() {
		return event.getBinlogFilename();
	}

	@Override
	public void setBinlogFilename(String binlogFilename) {
		event.setBinlogFilename(binlogFilename);
	}

	public abstract String getType();

	private Column findColumn(String name, Row r) {
		for(int i = 0; i < this.columns.length; i++ ) {
			if ( this.columns[i].getName().equals(name) )
				return r.getColumns().get(i);
		}
		return null;
	}

	private boolean rowMatchesFilter(Row r, Map<String, Object> filter) {
		if ( filter == null )
			return true;

		for (Map.Entry<String, Object> entry : filter.entrySet()) {
			String key = entry.getKey();
			Object value = entry.getValue();
			Column col = findColumn(key, r);

			if ( col == null )
				return false;

			if ( value instanceof Long ) {
				long v = ((Long) value).longValue();
				if ( col instanceof LongLongColumn ) {
					Long l = ((LongLongColumn) col).getValue();
					if ( v != l ) {
						return false;
					}
				} else if ( col instanceof LongColumn ||
						col instanceof Int24Column ||
						col instanceof ShortColumn ||
						col instanceof TinyColumn ) {
					Integer i = ((LongColumn) col).getValue();

					if ( v != i ) {
						return false;
					}
				}
			}
		}
		return true;

	}
	public List<Row> filteredRows(Map<String, Object> filter) {
		ArrayList<Row> res = new ArrayList<Row>();

		for(Row row : this.getRows()) {
			if ( rowMatchesFilter(row, filter))
				res.add(row);
		}
		return res;
	}

	public static ExodusAbstractRowsEvent buildEvent(AbstractRowEvent e,
			String tableName, ExodusColumnInfo[] columns, int idColumnOffset) {
		switch(e.getHeader().getEventType()) {
		case MySQLConstants.WRITE_ROWS_EVENT:
			return new ExodusWriteRowsEvent((WriteRowsEvent) e, tableName, columns);
		case MySQLConstants.UPDATE_ROWS_EVENT:
			return new ExodusUpdateRowsEvent((UpdateRowsEvent) e, tableName, columns);
		case MySQLConstants.DELETE_ROWS_EVENT:
			return new ExodusDeleteRowsEvent((DeleteRowsEvent) e, tableName, columns, idColumnOffset);
		}
		return null;
	}
	@Override
	public String toString() {
		return event.toString();
	}

	public abstract List<Row> getRows();
	public abstract String sqlOperationString();

	private String quoteString(String s) {
		String escaped = StringEscapeUtils.escapeSql(s);
		escaped = escaped.replaceAll("\n", "\\\\n");
		escaped = escaped.replaceAll("\r", "\\\\r");
		return "'" + escaped + "'";
	}
	private String columnToSql(Column c, String encoding) {
		if ( c instanceof NullColumn ) {
			return "NULL";
		} else if ( c instanceof BlobColumn ||
				c instanceof StringColumn ) {
			byte[] b = (byte[]) c.getValue();
			if ( encoding.equals("utf8") ) {
				String s = new String(b);
				return quoteString(s);
			} else {
				return "x'" +  Hex.encodeHexString( b ) + "'";
			}

		} else if ( c instanceof DateColumn ||
				    c instanceof YearColumn ) {
			return getDateFormatter().format(c.getValue());
		} else if ( c instanceof Datetime2Column ||
				    c instanceof DatetimeColumn ||
				    c instanceof Timestamp2Column ||
				    c instanceof TimestampColumn ) {
			return getDateTimeFormatter().format(c.getValue());
		} else if ( c instanceof Int24Column ||
				c instanceof LongColumn ||
			    c instanceof LongLongColumn  ||
			    c instanceof ShortColumn ||
			    c instanceof TinyColumn ||
			    c instanceof DoubleColumn ||
			    c instanceof FloatColumn ) {
			return c.getValue().toString();
		} else if ( c instanceof DecimalColumn ) {
			DecimalColumn dc = (DecimalColumn) c;
			return dc.getValue().toEngineeringString();
	    } else {
			return null;
		}

	}

	// useful for testing
	public List<Map<String, String>> rowAttributesAsSQL() {
		ArrayList<Map<String, String>> rows = new ArrayList<Map<String, String>>();

		for (Row r : getRows()) {
			HashMap<String, String> map = new HashMap<String, String>();
			for (int i = 0 ; i < columns.length; i++ ) {
				map.put(columns[i].getName(), columnToSql(r.getColumns().get(i), columns[i].getEncoding()));
			}
			rows.add(map);
		}
		return rows;
	}

	private void appendColumnNames(StringBuilder sql)
	{
		sql.append(" (");

		for(int i = 0 ; i < columns.length; i++) {
			sql.append("`" + columns[i].getName() + "`");
			if ( i < columns.length - 1 )
				sql.append(", ");
		}
		sql.append(")");

	}

	public String toSql(Map<String, Object> filter) {
		StringBuilder sql = new StringBuilder();
		List<Row> rows = filteredRows(filter);

		if ( rows.isEmpty() )
			return null;

		sql.append(sqlOperationString());
		sql.append("`" + tableName + "`");

		appendColumnNames(sql);

		sql.append(" VALUES ");

		for(Iterator<Row> rowIter = rows.iterator(); rowIter.hasNext(); ) {
			Row row = rowIter.next();
			int i = 0;

			sql.append("(");

			for(Iterator<Column> iter = row.getColumns().iterator(); iter.hasNext(); ) {
				Column c = iter.next();

				sql.append(columnToSql(c, columns[i].getEncoding()));

				if (iter.hasNext())
					sql.append(",");

				i++;
			}
			if ( rowIter.hasNext() ) {
				sql.append("),");
			} else {
				sql.append(")");
			}
		}

		return sql.toString();
	}

	public String toSql() {
		return this.toSql(null);
	}
}
