package com.zendesk.exodus;

import java.util.ArrayList;
import java.util.List;

import com.google.code.or.binlog.impl.event.UpdateRowsEvent;
import com.google.code.or.common.glossary.Pair;
import com.google.code.or.common.glossary.Row;

public class ExodusUpdateRowsEvent extends ExodusAbstractRowsEvent {
	private final UpdateRowsEvent event;
	public ExodusUpdateRowsEvent(UpdateRowsEvent e, String tableName, ExodusColumnSchemaDef[] columns) {
		super(e, tableName, columns);
		this.event = e;
	}
	@Override
	public List<Row> getRows() {
		ArrayList<Row> result = new ArrayList<Row>();
		for (Pair<Row> p : event.getRows()) {
			result.add(p.getAfter());
		}

		return result;
	}
	@Override
	public String sqlOperationString() {
		return "REPLACE INTO ";
	}
	@Override
	public String getType() {
		return "update";
	}
}
