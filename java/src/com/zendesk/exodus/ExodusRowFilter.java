package com.zendesk.exodus;

import java.util.List;

import com.google.code.or.binlog.impl.event.AbstractRowEvent;
import com.google.code.or.common.glossary.Column;
import com.google.code.or.common.glossary.Row;

public class ExodusRowFilter {
	private long value;
	private int tableId;
	private int colPosition;

	public ExodusRowFilter(int tableId, int colPosition, long value) {
		this.tableId = tableId;
		this.colPosition = colPosition;
		this.value = value;
	}
	
	public boolean matchesRow(Row r) {
		List<Column> c = r.getColumns();
		return false;
	}

	public int getTableId() {
		// TODO Auto-generated method stub
		return tableId;
	}

	public AbstractRowEvent filterEvent(AbstractRowEvent r) {
		// TODO Auto-generated method stub
		return null;
	}

}
