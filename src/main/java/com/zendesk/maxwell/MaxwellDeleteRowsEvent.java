package com.zendesk.maxwell;

import java.util.Iterator;
import java.util.List;
import com.google.code.or.binlog.impl.event.AbstractRowEvent;
import com.google.code.or.binlog.impl.event.DeleteRowsEvent;
import com.google.code.or.common.glossary.Row;
import com.zendesk.maxwell.schema.Table;

public class MaxwellDeleteRowsEvent extends ExodusAbstractRowsEvent {
	private final DeleteRowsEvent event;

	public MaxwellDeleteRowsEvent(AbstractRowEvent e, Table table, ExodusFilter f) {
		super(e, table, f);
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
	public String getType() {
		return "delete";
	}
}
