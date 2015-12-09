package com.zendesk.maxwell;

import java.util.ArrayList;
import java.util.List;
import java.util.*;

import com.google.code.or.binlog.impl.event.UpdateRowsEvent;
import com.google.code.or.binlog.impl.event.UpdateRowsEventV2;
import com.google.code.or.common.glossary.Column;
import com.google.code.or.common.glossary.Pair;
import com.google.code.or.common.glossary.Row;
import com.google.code.or.common.glossary.column.DatetimeColumn;
import com.zendesk.maxwell.schema.Table;
import com.zendesk.maxwell.schema.columndef.ColumnDef;

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

	private LinkedList<Pair<Row>> filteredRowsBeforeAndAfter;
	private boolean performedBeforeAndAfterFilter;

	private List<Pair<Row>> filteredRowsBeforeAndAfter() {
		if ( this.filter == null)
			return event.getRows();

		if ( performedBeforeAndAfterFilter )
			return filteredRowsBeforeAndAfter;

		filteredRowsBeforeAndAfter = new LinkedList<>();
		for ( Pair<Row> p : event.getRows()) {
			if ( this.filter.matchesRow(this, p.getAfter()) )
				filteredRowsBeforeAndAfter.add(p);
		}
		performedBeforeAndAfterFilter = true;
		return filteredRowsBeforeAndAfter;
	}

	@Override
	public List<RowMap> jsonMaps() {
		ArrayList<RowMap> list = new ArrayList<>();
		Object avalue;
		Object bvalue;
		for (Iterator<Pair<Row>> p = filteredRowsBeforeAndAfter().iterator(); p.hasNext(); ) {
			Pair<Row> rowpair = p.next();
			Row after = rowpair.getAfter();
			Row before = rowpair.getBefore();

			RowMap rowMap = buildRowMap();

			Iterator<Column> aftIter = after.getColumns().iterator();
			Iterator<Column> befIter = before.getColumns().iterator();
			Iterator<ColumnDef> defIter = table.getColumnList().iterator();
			while ( aftIter.hasNext() && defIter.hasNext() && befIter.hasNext() ) {
				Column a = aftIter.next();
				ColumnDef d = defIter.next();
				Column b = befIter.next();

				avalue = valueForJson(afterColumn);

				bvalue = valueForJson(beforeColumn);

				if (avalue != null) {
					avalue = d.asJSON(avalue);
					if ( !avalue.equals(d.asJSON(bvalue)) ) {
						bvalue = bvalue != null ? d.asJSON(bvalue) : bvalue;
						rowMap.putOldData(d.getName(), bvalue);
					}
				} else if (bvalue != null) {
					bvalue = d.asJSON(bvalue);
					rowMap.putOldData(d.getName(), bvalue);
				}
				rowMap.putData(d.getName(), avalue);
			}
			list.add(rowMap);
		}

		return list;
	}
}
