package com.zendesk.maxwell;

import java.util.ArrayList;
import java.util.List;
import java.util.*;

import com.google.code.or.binlog.impl.event.UpdateRowsEvent;
import com.google.code.or.binlog.impl.event.UpdateRowsEventV2;
import com.google.code.or.common.glossary.Column;
import com.google.code.or.common.glossary.Pair;
import com.google.code.or.common.glossary.Row;
import com.google.code.or.common.glossary.column.BitColumn;
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
	public List<Row> getRows() { // only for filterRows() at the moment.  need to refactor that.
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
		for (Pair<Row> p : filteredRowsBeforeAndAfter() ) {
			Row after = p.getAfter();
			Row before = p.getBefore();

			RowMap rowMap = buildRowMap();

			for ( ColumnWithDefinition cd : new ColumnWithDefinitionList(table, after, event.getUsedColumnsAfter())) {
				rowMap.putData(cd.definition.getName(), cd.asJSON());
			}

			for ( ColumnWithDefinition cd : new ColumnWithDefinitionList(table, before, event.getUsedColumnsBefore())) {
				String name = cd.definition.getName();
				Object beforeValue = cd.asJSON();

				if (!rowMap.hasData(name)) {
					// running in MINIMAL binlog row image mode.  Fill in after with before
					rowMap.putData(name, beforeValue);
				} else {
					if (!Objects.equals(rowMap.getData(name), beforeValue)) {
						rowMap.putOldData(name, beforeValue);
					}
				}
			}
			list.add(rowMap);
		}

		return list;
	}

	@Override
	protected BitColumn getUsedColumns() {
		return event.getUsedColumnsAfter(); // not actually used, since we override jsonMaps()
	}
}
