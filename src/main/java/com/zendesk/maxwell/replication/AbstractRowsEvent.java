package com.zendesk.maxwell.replication;

import java.util.*;
import java.util.regex.Pattern;

import com.google.code.or.binlog.BinlogEventV4Header;
import com.google.code.or.binlog.impl.event.AbstractRowEvent;
import com.google.code.or.common.glossary.Column;
import com.google.code.or.common.glossary.Row;
import com.google.code.or.common.glossary.column.BitColumn;
import com.zendesk.maxwell.MaxwellFilter;
import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.schema.ColumnWithDefinition;
import com.zendesk.maxwell.schema.ColumnWithDefinitionList;
import com.zendesk.maxwell.schema.Table;
import com.zendesk.maxwell.schema.columndef.ColumnDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// the main wrapper for a raw (AbstractRowEvent) binlog event.
// decorates the event with metadata info from Table,
// filters rows using the passed in MaxwellFilter,
// and ultimately outputs arrays of RowMap objects.

public abstract class AbstractRowsEvent extends AbstractRowEvent {
	static final Logger LOGGER = LoggerFactory.getLogger(AbstractRowsEvent.class);
	private final AbstractRowEvent event;
	private final Long heartbeat;

	protected final Table table;
	protected final String database;
	protected final MaxwellFilter filter;

	public AbstractRowsEvent(AbstractRowEvent e, Table table, MaxwellFilter f, Long heartbeat) {
		this.tableId = e.getTableId();
		this.event = e;
		this.header = e.getHeader();
		this.table = table;
		this.database = table.getDatabase();
		this.filter = f;
		this.heartbeat = heartbeat;
	}

	@Override
	public BinlogEventV4Header getHeader() {
		return event.getHeader();
	}

	public Table getTable() {
		return table;
	}

	public String getDatabase() {
		return database;
	}

	@Override
	public String getBinlogFilename() {
		return event.getBinlogFilename();
	}

	public BinlogPosition getNextBinlogPosition() {
		return new BinlogPosition(getHeader().getNextPosition(), getBinlogFilename(), heartbeat);
	}

	@Override
	public void setBinlogFilename(String binlogFilename) {
		event.setBinlogFilename(binlogFilename);
	}

	public boolean matchesFilter() {
		if ( filter == null )
			return true;

		return filter.matches(this.database, this.table.getName());
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
		List<Row> rows = getRows();

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

	protected RowMap buildRowMap() {
		return new RowMap(
				getType(),
				this.database,
				getTable().getName(),
				getHeader().getTimestamp() / 1000,
				table.getPKList(),
				this.getNextBinlogPosition());
	}

	public List<RowMap> jsonMaps() {
		ArrayList<RowMap> list = new ArrayList<>();

		for ( Row r : getRows() ) {
			RowMap rowMap = buildRowMap();

			for ( ColumnWithDefinition cd : new ColumnWithDefinitionList(table, r, getUsedColumns()) )
				rowMap.putData(cd.definition.getName(), cd.asJSON());

			list.add(rowMap);
		}

		return list;
	}

	protected abstract BitColumn getUsedColumns();
}
