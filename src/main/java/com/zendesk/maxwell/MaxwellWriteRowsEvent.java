package com.zendesk.maxwell;

import java.util.List;

import com.google.code.or.binlog.impl.event.WriteRowsEvent;
import com.google.code.or.binlog.impl.event.WriteRowsEventV2;
import com.google.code.or.common.glossary.Row;
import com.google.code.or.common.glossary.column.BitColumn;
import com.zendesk.maxwell.schema.Table;


public class MaxwellWriteRowsEvent extends MaxwellAbstractRowsEvent {
	private final WriteRowsEvent event;

	@Override
	public List<Row> getRows() {
		return event.getRows();
	}

	public MaxwellWriteRowsEvent(WriteRowsEvent e, Table t, MaxwellFilter f) {
		super(e, t, f);
		this.event = e;
	}

	public MaxwellWriteRowsEvent(WriteRowsEventV2 e2, Table table, MaxwellFilter filter) {
		super(e2, table, filter);
		WriteRowsEvent e =  new WriteRowsEvent(e2.getHeader());

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
