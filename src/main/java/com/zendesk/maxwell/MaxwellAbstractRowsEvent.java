package com.zendesk.maxwell;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;

import com.google.code.or.binlog.BinlogEventV4Header;
import com.google.code.or.binlog.impl.event.AbstractRowEvent;
import com.google.code.or.common.glossary.Column;
import com.google.code.or.common.glossary.Row;
import com.google.code.or.common.glossary.column.DatetimeColumn;
import com.zendesk.maxwell.schema.Database;
import com.zendesk.maxwell.schema.Table;
import com.zendesk.maxwell.schema.columndef.ColumnDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// the main wrapper for a raw (AbstractRowEvent) binlog event.
// decorates the event with metadata info from Table,
// filters rows using the passed in MaxwellFilter,
// and ultimately outputs arrays of json objects representing each row.

public abstract class MaxwellAbstractRowsEvent extends AbstractRowEvent {
	static final Logger LOGGER = LoggerFactory.getLogger(MaxwellAbstractRowsEvent.class);
	private final MaxwellFilter filter;
	private final AbstractRowEvent event;

	protected final Table table;
	protected final Database database;

	public MaxwellAbstractRowsEvent(AbstractRowEvent e, Table table, MaxwellFilter f) {
		this.tableId = e.getTableId();
		this.event = e;
		this.header = e.getHeader();
		this.table = table;
		this.database = table.getDatabase();
		this.filter = f;
	}

	@Override
	public BinlogEventV4Header getHeader() {
		return event.getHeader();
	}

	public Table getTable() {
		return table;
	}

	public Database getDatabase() {
		return database;
	}

	@Override
	public String getBinlogFilename() {
		return event.getBinlogFilename();
	}

	public BinlogPosition getNextBinlogPosition() {
		return new BinlogPosition(getHeader().getNextPosition(), getBinlogFilename());
	}

	@Override
	public void setBinlogFilename(String binlogFilename) {
		event.setBinlogFilename(binlogFilename);
	}

	public boolean matchesFilter() {
		if ( filter == null )
			return true;

		return filter.matches(this);
	}

	public abstract String getType();

	public Column findColumn(String name, Row r) {
		int i = table.findColumnIndex(name);
		if ( i > 0 )
			return r.getColumns().get(i);
		else
			return null;
	}

	@Override
	public String toString() {
		return event.toString();
	}

	public abstract List<Row> getRows();
	public abstract String sqlOperationString();

	private LinkedList<Row> filteredRows;
	private boolean performedFilter = false;

	protected List<Row> filteredRows() {
		if ( this.filter == null )
			return getRows();

		if ( performedFilter )
			return filteredRows;

		filteredRows = new LinkedList<>();
		for ( Row r : getRows()) {
			if ( this.filter.matchesRow(this, r) )
				filteredRows.add(r);
		}
		performedFilter = true;

		return filteredRows;
	}

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

	public String toSQL() {
		StringBuilder sql = new StringBuilder();
		List<Row> rows = filteredRows();

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

	public List<RowMap> jsonMaps() {
		ArrayList<RowMap> list = new ArrayList<>();
		Object value;

		for ( Iterator<Row> ri = filteredRows().iterator() ; ri.hasNext(); ) {
			Row r = ri.next();

			RowMap rowMap = new RowMap(
					getType(),
					getDatabase().getName(),
					getTable().getName(),
					getHeader().getTimestamp() / 1000,
					table.getPKList(),
					this.getNextBinlogPosition());

			Iterator<Column> colIter = r.getColumns().iterator();
			Iterator<ColumnDef> defIter = table.getColumnList().iterator();

			while ( colIter.hasNext() && defIter.hasNext() ) {
				Column c = colIter.next();
				ColumnDef d = defIter.next();

				if ( c == null ) {
					continue;
				} else if (c instanceof DatetimeColumn) {
					value = ((DatetimeColumn) c).getLongValue();
				} else {
					value = c.getValue();
				}

				if ( value != null )
					value = d.asJSON(value);

				rowMap.putData(d.getName(), value);
			}
			list.add(rowMap);
		}

		return list;
	}
}
