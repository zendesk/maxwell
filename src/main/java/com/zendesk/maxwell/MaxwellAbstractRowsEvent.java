package com.zendesk.maxwell;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONObject;

import com.google.code.or.binlog.BinlogEventV4Header;
import com.google.code.or.binlog.impl.event.AbstractRowEvent;
import com.google.code.or.common.glossary.Column;
import com.google.code.or.common.glossary.Row;
import com.zendesk.maxwell.schema.Table;
import com.zendesk.maxwell.schema.columndef.ColumnDef;

public abstract class MaxwellAbstractRowsEvent extends AbstractRowEvent {
	private final MaxwellFilter filter;
	private final AbstractRowEvent event;
	protected final Table table;

	public MaxwellAbstractRowsEvent(AbstractRowEvent e, Table table, MaxwellFilter f) {
		this.tableId = e.getTableId();
		this.event = e;
		this.header = e.getHeader();
		this.table = table;
		this.filter = f;
	}

	@Override
	public BinlogEventV4Header getHeader() {
		return event.getHeader();
	}

	public Table getTable() {
		return table;
	}

	@Override
	public String getBinlogFilename() {
		return event.getBinlogFilename();
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

	// the equivalent of "asJSON" -- convert the row to an array of
	// hashes
	public List<Map<String, Object>> jsonMaps() {
		List<Map<String, Object>> list = new ArrayList<>();

		for ( Row r : filteredRows()) {
			HashMap<String, Object> rowMap = new HashMap<>();
			Iterator<Column> colIter = r.getColumns().iterator();
			Iterator<ColumnDef> defIter = table.getColumnList().iterator();

			while ( colIter.hasNext() && defIter.hasNext() ) {
				Column c = colIter.next();
				ColumnDef d = defIter.next();

				rowMap.put(d.getName(), d.asJSON(c.getValue()));
			}
			list.add(rowMap);
		}

		return list;
	}
}
