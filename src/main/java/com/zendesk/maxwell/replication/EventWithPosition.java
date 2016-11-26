package com.zendesk.maxwell.replication;

import com.github.shyiko.mysql.binlog.event.*;

public class EventWithPosition {
	private final Event event;
	private final BinlogPosition position;

	public EventWithPosition(Event event, BinlogPosition position) {
		this.event = event;
		this.position = position;
	}

	public Event getEvent() {
		return event;
	}

	public BinlogPosition getPosition() {
		return position;
	}

	public Long getTableID() {
		EventData data = event.getData();
		switch ( event.getHeader().getEventType() ) {
			case WRITE_ROWS:
				return ((WriteRowsEventData) data).getTableId();
			case UPDATE_ROWS:
				return ((UpdateRowsEventData) data).getTableId();
			case DELETE_ROWS:
				return ((DeleteRowsEventData) data).getTableId();
		}
		return null;
	}
}
