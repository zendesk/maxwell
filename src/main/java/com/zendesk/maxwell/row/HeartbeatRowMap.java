package com.zendesk.maxwell.row;

import com.zendesk.maxwell.producer.MaxwellOutputConfig;
import com.zendesk.maxwell.replication.BinlogPosition;

import java.io.IOException;
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

	@Override
	public String toJSON(MaxwellOutputConfig outputConfig) throws IOException {
		return null;
	}

	@Override
	public String toJSON() throws IOException {
		return null;
	}

	@Override
	public boolean isTXCommit() {
		return true;
	}
}
