package com.zendesk.exodus;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.code.or.binlog.impl.event.AbstractRowEvent;
import com.google.code.or.binlog.impl.event.DeleteRowsEvent;
import com.google.code.or.common.glossary.Row;
import com.zendesk.exodus.schema.Table;

public class ExodusDeleteRowsEvent extends ExodusAbstractRowsEvent {
	private final DeleteRowsEvent event;

	public ExodusDeleteRowsEvent(AbstractRowEvent e, Table table) {
		super(e, table);
		this.event = (DeleteRowsEvent) e;
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
	public String toSql(Map <String, Object> filter) {
		List<Row> rows = filteredRows(filter);

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
	public String getType() {
		return "delete";
	}
}
