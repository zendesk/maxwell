package com.zendesk.maxwell.row;

import com.zendesk.maxwell.producer.MaxwellOutputConfig;
import com.zendesk.maxwell.replication.BinlogPosition;
import com.zendesk.maxwell.replication.Position;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by ben on 9/7/16.
 */
public class HeartbeatRowMap extends RowMap {
	public HeartbeatRowMap(String database, Position position) {
		super("heartbeat", database, "heartbeats", position.getLastHeartbeatRead(), new ArrayList<String>(), position);
	}

	public static HeartbeatRowMap valueOf(String database, Position position) {
		return new HeartbeatRowMap(database, position);
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

	@Override
	public boolean shouldOutput(MaxwellOutputConfig outputConfig) {
		return false;
	}
}
