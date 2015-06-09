package com.zendesk.maxwell;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.json.JSONObject;

import com.google.code.or.binlog.BinlogEventV4Header;
import com.google.code.or.binlog.impl.event.AbstractRowEvent;
import com.google.code.or.common.glossary.Column;
import com.google.code.or.common.glossary.Row;
import com.google.code.or.common.glossary.column.DatetimeColumn;
import com.zendesk.maxwell.schema.Database;
import com.zendesk.maxwell.schema.Table;
import com.zendesk.maxwell.schema.columndef.ColumnDef;

// the main wrapper for a raw (AbstractRowEvent) binlog event.
// decorates the event with metadata info from Table,
// filters rows using the passed in MaxwellFilter,
// and ultimately outputs arrays of json objects representing each row.

public abstract class MaxwellAbstractRowsEvent extends AbstractRowEvent {
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

	class RowMap extends HashMap<String, Object> {
		private final HashMap<String, Object> data;

		public RowMap() {
			this.data = new HashMap<String, Object>();
			this.put("data", this.data);
		}

		public void setRowType(String type) {
			this.put("type", type);
		}

		public void putData(String key, Object value) {
			this.data.put(key,  value);
		}

		public void setTable(String name) {
			this.put("table", name);
		}

		public void setDatabase(String name) {
			this.put("database", name);
		}

		public void setTimestamp(Long l) {
			this.put("ts", l);
		}

		public Object getData(String string) {
			return this.data.get(string);
		}
	}

	public List<RowMap> jsonMaps() {
		ArrayList<RowMap> list = new ArrayList<>();
		Object value;
		for ( Row r : filteredRows()) {
			RowMap rowMap = new RowMap();

			rowMap.setRowType(getType());
			rowMap.setTable(getTable().getName());
			rowMap.setDatabase(getDatabase().getName());
			rowMap.setTimestamp(getHeader().getTimestamp() / 1000);

			Iterator<Column> colIter = r.getColumns().iterator();
			Iterator<ColumnDef> defIter = table.getColumnList().iterator();

			while ( colIter.hasNext() && defIter.hasNext() ) {
				Column c = colIter.next();
				ColumnDef d = defIter.next();

				if ( c instanceof DatetimeColumn )
					value = ((DatetimeColumn) c).getLongValue();
				else
					value = c.getValue();

				if ( value != null )
					value = d.asJSON(value);

				rowMap.putData(d.getName(), value);
			}
			list.add(rowMap);
		}

		return list;
	}

	public List<JSONObject> toJSONObjects() {
		ArrayList<JSONObject> list = new ArrayList<>();

		for ( RowMap map : jsonMaps() ) {
			list.add(new MaxwellJSONObject(map));
		}
		return list;
	}

	public List<String> toJSONStrings() {
		ArrayList<String> list = new ArrayList<>();

		for ( RowMap map : jsonMaps() ) {
			list.add(new MaxwellJSONObject(map).toString());
		}
		return list;
	}

	public List<String> getPKStrings() {
		ArrayList<String> list = new ArrayList<>();

		for ( Row r : filteredRows()) {
			list.add(new MaxwelllPKJSONObject(table.getDatabase().getName(),
										      table.getName(),
										      getPKMapForRow(r)).toString());
		}

		return list;
	}

	private Map<String, Object> getPKMapForRow(Row r) {
		HashMap<String, Object> map = new HashMap<>();

		if ( table.getPKList().isEmpty() ) {
			map.put("_uuid", java.util.UUID.randomUUID().toString());
		}

		for ( String pk : table.getPKList() ) {
			int idx = table.findColumnIndex(pk);

			Column column = r.getColumns().get(idx);
			ColumnDef def = table.getColumnList().get(idx);

			map.put(pk, def.asJSON(column.getValue()));
		}
		return map;
	}
}
