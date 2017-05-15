package com.zendesk.maxwell.replication;

import java.util.List;

import com.google.code.or.binlog.impl.event.WriteRowsEventV2;
import com.google.code.or.common.glossary.Row;
import com.google.code.or.common.glossary.column.BitColumn;
import com.zendesk.maxwell.MaxwellFilter;
import com.zendesk.maxwell.schema.Table;


public class WriteRowsEvent extends AbstractRowsEvent {
	private final com.google.code.or.binlog.impl.event.WriteRowsEvent event;

	@Override
	public List<Row> getRows() {
		return event.getRows();
	}

	public WriteRowsEvent(com.google.code.or.binlog.impl.event.WriteRowsEvent e, Table t, MaxwellFilter f, long lastHeartbeat) {
		super(e, t, f, lastHeartbeat);
		this.event = e;
	}

	public WriteRowsEvent(WriteRowsEventV2 e2, Table table, MaxwellFilter filter, long lastHeartbeat) {
		super(e2, table, filter, lastHeartbeat);
		com.google.code.or.binlog.impl.event.WriteRowsEvent e =  new com.google.code.or.binlog.impl.event.WriteRowsEvent(e2.getHeader());

		e.setBinlogFilename(e2.getBinlogFilename());
		e.setColumnCount(e2.getColumnCount());
		e.setRows(e2.getRows());
		e.setTableId(e2.getTableId());
		e.setUsedColumns(e2.getUsedColumns());
		e.setReserved(e2.getReserved());
		this.event = e;
	}

	@Override
	public String sqlOperationString() {
		return "REPLACE INTO ";
	}

	@Override
	protected BitColumn getUsedColumns() {
		return event.getUsedColumns();
	}

	@Override
	public String getType() {
		return "insert";
	}
}
