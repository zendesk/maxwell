package com.zendesk.maxwell.replication;

import com.github.shyiko.mysql.binlog.event.Event;

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
}
