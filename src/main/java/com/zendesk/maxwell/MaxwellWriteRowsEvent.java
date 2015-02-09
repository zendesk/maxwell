package com.zendesk.maxwell;

import java.util.List;

import com.google.code.or.binlog.impl.event.WriteRowsEvent;
import com.google.code.or.common.glossary.Row;
import com.zendesk.maxwell.schema.Table;


public class MaxwellWriteRowsEvent extends ExodusAbstractRowsEvent {
	private final WriteRowsEvent event;

	@Override
	public List<Row> getRows() {
		return event.getRows();
	}

	public MaxwellWriteRowsEvent(WriteRowsEvent e, Table t, ExodusFilter f) {
		super(e, t, f);
		this.event = e;
	}

	@Override
	public String sqlOperationString() {
		return "REPLACE INTO ";
	}

	@Override
	public String getType() {
		return "insert";
	}
}
