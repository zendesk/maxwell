package com.zendesk.exodus;

import java.math.BigInteger;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang.StringEscapeUtils;

import com.google.code.or.binlog.BinlogEventV4Header;
import com.google.code.or.binlog.impl.event.AbstractRowEvent;
import com.google.code.or.common.glossary.Column;
import com.google.code.or.common.glossary.Row;
import com.google.code.or.common.glossary.column.*;
import com.zendesk.exodus.schema.Table;
import com.zendesk.exodus.schema.columndef.ColumnDef;

public abstract class ExodusAbstractRowsEvent extends AbstractRowEvent {
	private static final TimeZone tz = TimeZone.getTimeZone("UTC");

	private final AbstractRowEvent event;
	protected final Table table;

	public ExodusAbstractRowsEvent(AbstractRowEvent e, Table table) {
		this.tableId = e.getTableId();
		this.event = e;
		this.header = e.getHeader();
		this.table = table;
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
		int i = table.findColumnIndex(name);
		if ( i > 0 )
			return r.getColumns().get(i);
		else
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

	@Override
	public String toString() {
		return event.toString();
	}

	public abstract List<Row> getRows();
	public abstract String sqlOperationString();

	private void appendColumnNames(StringBuilder sql)
	{
		sql.append(" (");

		for(Iterator<ColumnDef> i = table.getColumnList().iterator(); i.hasNext();) {
			ColumnDef c = i.next();
			sql.append("`" + c.getName() + "`");
			if ( i.hasNext() )
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
		sql.append("`" + table.getName() + "`");

		appendColumnNames(sql);

		sql.append(" VALUES ");

		Iterator<Row> rowIter = rows.iterator();

		while(rowIter.hasNext()) {
			Row row = rowIter.next();

			sql.append("(");

			Iterator<Column> colIter = row.getColumns().iterator();
			Iterator<ColumnDef> defIter = table.getColumnList().iterator();

			while ( colIter.hasNext() && defIter.hasNext() ) {
				Column c = colIter.next();
				ColumnDef d = defIter.next();


				sql.append(d.toSQL(c.getValue()));

				 if (colIter.hasNext())
					sql.append(",");
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
