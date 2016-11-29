package com.zendesk.maxwell.replication;

import java.util.ArrayList;
import java.util.List;
import java.util.*;

import com.google.code.or.binlog.impl.event.UpdateRowsEventV2;
import com.google.code.or.common.glossary.Pair;
import com.google.code.or.common.glossary.Row;
import com.google.code.or.common.glossary.column.BitColumn;
import com.zendesk.maxwell.MaxwellFilter;
import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.schema.ColumnWithDefinition;
import com.zendesk.maxwell.schema.ColumnWithDefinitionList;
import com.zendesk.maxwell.schema.Table;

public class UpdateRowsEvent extends AbstractRowsEvent {
	private final com.google.code.or.binlog.impl.event.UpdateRowsEvent event;

	public UpdateRowsEvent(com.google.code.or.binlog.impl.event.UpdateRowsEvent e, Table t, MaxwellFilter f, Long heartbeat) {
		super(e, t, f, heartbeat);
		this.event = e;
	}

	public UpdateRowsEvent(UpdateRowsEventV2 e2, Table table, MaxwellFilter filter, Long heartbeat) {
		super(e2, table, filter, heartbeat);
		com.google.code.or.binlog.impl.event.UpdateRowsEvent e =  new com.google.code.or.binlog.impl.event.UpdateRowsEvent(e2.getHeader());

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

	@Override
	public List<RowMap> jsonMaps() {
		ArrayList<RowMap> list = new ArrayList<>();
		for (Pair<Row> p : event.getRows() ) {
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
					/*
					   If we find a column in the BEFORE image that's *not* present in the AFTER image,
					   we're running in binlog_row_image = MINIMAL.  In this case, the BEFORE image acts
					   as a sort of WHERE clause to update rows with the new values (present in the AFTER image).

					   In order to reconstruct as much of the row as posssible, here we fill in
					   missing data in the rowMap with values from the BEFORE image
					 */
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
