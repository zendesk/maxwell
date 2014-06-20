package com.zendesk.exodus;

import java.util.ArrayList;
import java.util.List;

import com.google.code.or.binlog.impl.event.UpdateRowsEvent;
import com.google.code.or.binlog.impl.event.WriteRowsEvent;
import com.google.code.or.common.glossary.Pair;
import com.google.code.or.common.glossary.Row;

public class ExodusUpdateRowsEvent extends ExodusAbstractRowsEvent {
	private UpdateRowsEvent event;
	public ExodusUpdateRowsEvent(UpdateRowsEvent e, String tableName, String columnNames) {
		super(e, tableName, columnNames);
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

}
