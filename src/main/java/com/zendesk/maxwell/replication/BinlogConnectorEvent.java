package com.zendesk.maxwell.replication;

import com.github.shyiko.mysql.binlog.event.*;
import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.schema.Table;
import com.zendesk.maxwell.schema.columndef.ColumnDef;

import java.io.Serializable;
import java.util.*;

public class BinlogConnectorEvent {
	private BinlogPosition position;
	private BinlogPosition nextPosition;
	private final Event event;
	private final String gtidSetStr;
	private final String gtid;

	public BinlogConnectorEvent(Event event, String filename, String gtidSetStr, String gtid) {
		this.event = event;
		this.gtidSetStr = gtidSetStr;
		this.gtid = gtid;
		EventHeaderV4 hV4 = (EventHeaderV4) event.getHeader();
		this.nextPosition = new BinlogPosition(gtidSetStr, gtid, hV4.getNextPosition(), filename, null);
		this.position = new BinlogPosition(gtidSetStr, gtid, hV4.getPosition(), filename, null);
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

	public void setLastHeartbeat(Long lastHeartbeat) {
		this.position     = new BinlogPosition(gtidSetStr, gtid, this.position.getOffset(), this.position.getFile(), lastHeartbeat);
		this.nextPosition = new BinlogPosition(gtidSetStr, gtid, this.position.getOffset(), this.position.getFile(), lastHeartbeat);
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
		int dataIdx = 0, colIdx = 0;

		for ( ColumnDef cd : table.getColumnList() ) {
			if ( includedColumns.get(colIdx) ) {
				Object json = null;
				if ( data[dataIdx] != null ) {
					json = cd.asJSON(data[dataIdx]);
				}
				row.putData(cd.getName(), json);
				dataIdx++;
			}
			colIdx++;
		}
	}

	private void writeOldData(Table table, RowMap row, Serializable[] oldData, BitSet oldIncludedColumns) {
		int dataIdx = 0, colIdx = 0;

		for ( ColumnDef cd : table.getColumnList() ) {
			if ( oldIncludedColumns.get(colIdx) ) {
				Object json = null;
				if ( oldData[dataIdx] != null ) {
					json = cd.asJSON(oldData[dataIdx]);
				}

				if (!row.hasData(cd.getName())) {
					/*
					   If we find a column in the BEFORE image that's *not* present in the AFTER image,
					   we're running in binlog_row_image = MINIMAL.  In this case, the BEFORE image acts
					   as a sort of WHERE clause to update rows with the new values (present in the AFTER image),
					   In this case we should put what's in the "before" image into the "data" section, not the "old".
					 */
					row.putData(cd.getName(), json);
				} else {
					if (!Objects.equals(row.getData(cd.getName()), json)) {
						row.putOldData(cd.getName(), json);
					}
				}
				dataIdx++;
			}
			colIdx++;
		}
	}

	private RowMap buildRowMap(String type, Serializable[] data, Table table, BitSet includedColumns) {
		RowMap map = new RowMap(
			type,
			table.getDatabase(),
			table.getName(),
			event.getHeader().getTimestamp() / 1000,
			table.getPKList(),
			nextPosition
		);

		writeData(table, map, data, includedColumns);
		return map;
	}

	public List<RowMap> jsonMaps(Table table) {
		ArrayList<RowMap> list = new ArrayList<>();

		switch ( getType() ) {
			case WRITE_ROWS:
			case EXT_WRITE_ROWS:
				for ( Serializable[] data : writeRowsData().getRows() ) {
					list.add(buildRowMap("insert", data, table, writeRowsData().getIncludedColumns()));
				}
				break;
			case DELETE_ROWS:
			case EXT_DELETE_ROWS:
				for ( Serializable[] data : deleteRowsData().getRows() ) {
					list.add(buildRowMap("delete", data, table, deleteRowsData().getIncludedColumns()));
				}
				break;
			case UPDATE_ROWS:
			case EXT_UPDATE_ROWS:
				for ( Map.Entry<Serializable[], Serializable[]> e : updateRowsData().getRows() ) {
					Serializable[] data = e.getValue();
					Serializable[] oldData = e.getKey();

					RowMap r = buildRowMap("update", data, table, updateRowsData().getIncludedColumns());
					writeOldData(table, r, oldData, updateRowsData().getIncludedColumnsBeforeUpdate());
					list.add(r);
				}
				break;
		}

		return list;
	}
}
