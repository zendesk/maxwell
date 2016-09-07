package com.zendesk.maxwell;

import java.util.ArrayList;

/**
 * Created by ben on 9/7/16.
 */
public class HeartbeatRowMap extends RowMap {
	public HeartbeatRowMap(String database, BinlogPosition position) {
		super("heartbeat", database, "heartbeats", position.getHeartbeat(), new ArrayList<String>(), position);
	}

	public static HeartbeatRowMap valueOf(String database, BinlogPosition position, long heartbeatValue) {
		BinlogPosition p = new BinlogPosition(position.getOffset(), position.getFile(), heartbeatValue);
		return new HeartbeatRowMap(database, p);
	}
}
