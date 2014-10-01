package com.zendesk.exodus;

import java.util.List;

import com.google.code.or.binlog.impl.event.WriteRowsEvent;
import com.google.code.or.common.glossary.Row;


public class ExodusWriteRowsEvent extends ExodusAbstractRowsEvent {
	private final WriteRowsEvent event;

	@Override
	public List<Row> getRows() {
		return event.getRows();
	}

	public ExodusWriteRowsEvent(WriteRowsEvent e, String tableName, ExodusColumnSchemaDef[] columns) {
		super(e, tableName, columns);
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
