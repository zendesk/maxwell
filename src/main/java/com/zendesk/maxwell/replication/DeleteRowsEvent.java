package com.zendesk.maxwell.replication;

import java.util.Iterator;
import java.util.List;

import com.google.code.or.binlog.impl.event.DeleteRowsEventV2;
import com.google.code.or.common.glossary.Row;
import com.google.code.or.common.glossary.column.BitColumn;
import com.zendesk.maxwell.MaxwellFilter;
import com.zendesk.maxwell.schema.Table;

public class DeleteRowsEvent extends AbstractRowsEvent {
	private final com.google.code.or.binlog.impl.event.DeleteRowsEvent event;

	public DeleteRowsEvent(com.google.code.or.binlog.impl.event.DeleteRowsEvent e, Table table, MaxwellFilter f, long lastHeartbeat) {
		super(e, table, f, lastHeartbeat);
		this.event = e;
	}

	public DeleteRowsEvent(DeleteRowsEventV2 e2, Table table, MaxwellFilter filter, long lastHeartbeat) {
		super(e2, table, filter, lastHeartbeat);

		com.google.code.or.binlog.impl.event.DeleteRowsEvent e =  new com.google.code.or.binlog.impl.event.DeleteRowsEvent(e2.getHeader());

		e.setBinlogFilename(e2.getBinlogFilename());
		e.setColumnCount(e2.getColumnCount());
		e.setRows(e2.getRows());
		e.setTableId(e2.getTableId());
		e.setUsedColumns(e2.getUsedColumns());
		e.setReserved(e2.getReserved());
		this.event = e;
	}

	@Override
	public List<Row> getRows() {
		return event.getRows();
	}

	@Override
	public String sqlOperationString() {
		return null;
	}

	@Override
	public String toSQL() {
		List<Row> rows = getRows();

		if ( rows.isEmpty()) {
			return null;
		}

		StringBuilder s = new StringBuilder();
		s.append("DELETE FROM `" + this.table.getName() + "` WHERE id in (");

		for(Iterator<Row> rowIter = getRows().iterator(); rowIter.hasNext(); ) {
			int pkIndex = this.table.getPKIndex();
			s.append(rowIter.next().getColumns().get(pkIndex).toString());
			if ( rowIter.hasNext() )
				s.append(",");
		}
		s.append(")");
		return s.toString();
	}

	@Override
	protected BitColumn getUsedColumns() {
		return event.getUsedColumns();
	}

	@Override
	public String getType() {
		return "delete";
	}
}
