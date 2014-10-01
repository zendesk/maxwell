package com.zendesk.exodus;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.code.or.binlog.impl.event.DeleteRowsEvent;
import com.google.code.or.common.glossary.Row;

public class ExodusDeleteRowsEvent extends ExodusAbstractRowsEvent {
	private final DeleteRowsEvent event;
	private final int idColumnPosition;

	@Override
	public List<Row> getRows() {
		return event.getRows();
	}


	public ExodusDeleteRowsEvent(DeleteRowsEvent e, String tableName, ExodusColumnInfo[] columns, int idColumnPosition) {
		super(e, tableName, columns);
		this.event = e;
		this.idColumnPosition = idColumnPosition;
	}

	@Override
	public String sqlOperationString() {
		return null;
	}

	@Override
	public String toSql(Map <String, Object> filter) {
		List<Row> rows = filteredRows(filter);

		if ( rows.isEmpty()) {
			return null;
		}

		StringBuilder s = new StringBuilder();
		s.append("DELETE FROM `" + this.tableName + "` WHERE id in (");

		for(Iterator<Row> rowIter = getRows().iterator(); rowIter.hasNext(); ) {
			s.append(rowIter.next().getColumns().get(idColumnPosition).toString());
			if ( rowIter.hasNext() )
				s.append(",");
		}
		s.append(")");
		return s.toString();
	}

	@Override
	public String getType() {
		return "delete";
	}
}
