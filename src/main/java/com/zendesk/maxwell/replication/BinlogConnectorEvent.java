package com.zendesk.maxwell.replication;

import com.github.shyiko.mysql.binlog.event.*;
import com.zendesk.maxwell.MaxwellFilter;
import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.schema.Table;
import com.zendesk.maxwell.schema.columndef.ColumnDef;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;

public class BinlogConnectorEvent {
	private final Event event;
	private final BinlogPosition position;
	private final BinlogPosition nextPosition;

	public BinlogConnectorEvent(Event event, String filename) {
		this.event = event;
		EventHeaderV4 hV4 = (EventHeaderV4) event.getHeader();
		this.nextPosition = BinlogPosition.at(hV4.getNextPosition(), filename);
		this.position = BinlogPosition.at(hV4.getPosition(), filename);
	}

	public Event getEvent() {
		return event;
	}

	public WriteRowsEventData writeRowsData() {
		return (WriteRowsEventData) event.getData();
	}

	public UpdateRowsEventData updateRowsData() {
		return (UpdateRowsEventData) event.getData();
	}

	public DeleteRowsEventData deleteRowsData() {
		return (DeleteRowsEventData) event.getData();
	}

	public QueryEventData queryData() {
		return (QueryEventData) event.getData();
	}

	public XidEventData xidData() {
		return (XidEventData) event.getData();
	}

	public TableMapEventData tableMapData() {
		return (TableMapEventData) event.getData();
	}

	public BinlogPosition getPosition() {
		return position;
	}

	public EventType getType() {
		return event.getHeader().getEventType();
	}

	public Long getTableID() {
		EventData data = event.getData();
		switch ( event.getHeader().getEventType() ) {
			case EXT_WRITE_ROWS:
			case WRITE_ROWS:
				return ((WriteRowsEventData) data).getTableId();
			case EXT_UPDATE_ROWS:
			case UPDATE_ROWS:
				return ((UpdateRowsEventData) data).getTableId();
			case EXT_DELETE_ROWS:
			case DELETE_ROWS:
				return ((DeleteRowsEventData) data).getTableId();
			case TABLE_MAP:
				return ((TableMapEventData) data).getTableId();
		}
		return null;
	}

	private void writeData(Table table, RowMap row, Serializable[] data, BitSet includedColumns) {
		int i = 0, j = 0;
		for ( ColumnDef cd : table.getColumnList() ) {
			if ( includedColumns.get(j) ) {
				Object json = null;
				if ( data[i] != null ) {
					json = cd.asJSON(data[i]);
				}
				row.putData(cd.getName(), json);
				i++;
			}
			j++;
		}
	}

	public List<RowMap> jsonMaps(Table table, MaxwellFilter filter) {
		ArrayList<RowMap> list = new ArrayList<>();

		String type = null;
		List<Serializable[]> rowData = null;
		List<Serializable[]> oldRowData = null;
		BitSet includedColumns = null;

		switch ( getType() ) {
			case WRITE_ROWS:
			case EXT_WRITE_ROWS:
				type = "insert";
				rowData = writeRowsData().getRows();
				includedColumns = writeRowsData().getIncludedColumns();
				break;
			case DELETE_ROWS:
			case EXT_DELETE_ROWS:
				type = "delete";
				rowData = deleteRowsData().getRows();
				includedColumns = deleteRowsData().getIncludedColumns();
				break;
			case UPDATE_ROWS:
			case EXT_UPDATE_ROWS:
				type = "update";
				rowData = new ArrayList<>();
				oldRowData = new ArrayList<>();
				includedColumns = updateRowsData().getIncludedColumns();
				for ( Map.Entry<Serializable[], Serializable[]> e : updateRowsData().getRows() ) {
					rowData.add(e.getValue());
					oldRowData.add(e.getKey());
				}
		}

		for ( Serializable[] data : rowData ) {
			RowMap map = new RowMap(
				type,
				table.getDatabase(),
				table.getName(),
				event.getHeader().getTimestamp() / 1000,
				table.getPKList(),
				nextPosition,
				filter.getExcludeColumns()
			);

			writeData(table, map, data, includedColumns);
			list.add(map);
		}

		return list;
	}
}
