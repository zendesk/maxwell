package com.zendesk.maxwell;

import java.util.ArrayList;
import java.util.List;

import com.google.code.or.binlog.impl.event.UpdateRowsEvent;
import com.google.code.or.binlog.impl.event.UpdateRowsEventV2;
import com.google.code.or.common.glossary.Column;
import com.google.code.or.common.glossary.Pair;
import com.google.code.or.common.glossary.Row;
import com.google.code.or.common.glossary.column.BitColumn;
import com.zendesk.maxwell.schema.Table;

public class MaxwellUpdateRowsEvent extends MaxwellAbstractRowsEvent {
	private final UpdateRowsEvent event;

	public MaxwellUpdateRowsEvent(UpdateRowsEvent e, Table t, MaxwellFilter f) {
		super(e, t, f);
		this.event = e;
	}

	public MaxwellUpdateRowsEvent(UpdateRowsEventV2 e2, Table table, MaxwellFilter filter) {
		super(e2, table, filter);
		UpdateRowsEvent e =  new UpdateRowsEvent(e2.getHeader());

		e.setBinlogFilename(e2.getBinlogFilename());
		e.setColumnCount(e2.getColumnCount());
		e.setRows(e2.getRows());
		e.setTableId(e2.getTableId());
		e.setUsedColumnsAfter(e2.getUsedColumnsAfter());
		e.setUsedColumnsBefore(e2.getUsedColumnsBefore());
		e.setReserved(e2.getReserved());
		this.event = e;
	}

	private boolean hasUnsetColumns() {
		for ( int i = 0 ; i < event.getColumnCount().intValue(); i++ ) {
			if ( !event.getUsedColumnsAfter().get(i) ) {
				return true;
			}
		}
		return false;
	}


	@Override
	public List<Row> getRows() {
		ArrayList<Row> result = new ArrayList<Row>();

		int nColumns = event.getColumnCount().intValue();
		boolean hasUnset = hasUnsetColumns();

		BitColumn usedBefore = event.getUsedColumnsBefore();
		BitColumn usedAfter  = event.getUsedColumnsAfter();

		for (Pair<Row> p : event.getRows()) {
			if ( hasUnset ) {
				int beforeIdx = 0;
				int afterIdx = 0;
				List<Column> beforeColumns = p.getBefore().getColumns();
				List<Column> afterColumns  = p.getAfter().getColumns();

				List<Column> c = new ArrayList<Column>(nColumns);

				for ( int i = 0 ; i < nColumns; i++ ) {
					if ( usedAfter.get(i) )
						c.add(afterColumns.get(afterIdx));
					else if ( usedBefore.get(i) )
						c.add(beforeColumns.get(beforeIdx));
					else
						c.add(null);

					if ( usedAfter.get(i) )
						afterIdx++;

					if ( usedBefore.get(i) )
						beforeIdx++;
				}
				result.add(new Row(c));
			} else {
				result.add(p.getAfter());
			}
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
