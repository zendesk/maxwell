package com.zendesk.maxwell.replication;

import com.zendesk.maxwell.row.HeartbeatRowMap;
import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.util.RunLoopProcess;

import java.sql.SQLException;
import java.util.Objects;

public abstract class AbstractReplicator extends RunLoopProcess {

	protected final String clientID;
	protected Long lastHeartbeatRead;

	public AbstractReplicator(String clientID) {
		this.clientID = clientID;
	}

	/**
	 * Possibly convert a RowMap object into a HeartbeatRowMap
	 *
	 * Process a rowmap that represents a write to `maxwell`.`positions`.
	 * If it's a write for a different client_id, or it's not a heartbeat,
	 * we return just the RowMap.  Otherwise, we transform it into a HeartbeatRowMap
	 * and set lastHeartbeatRead.
	 *
	 * @return either a RowMap or a HeartbeatRowMap
	 */
	protected RowMap processHeartbeats(RowMap row) throws SQLException {
		String hbClientID = (String) row.getData("client_id");
		if ( !Objects.equals(hbClientID, this.clientID) )
			return row;

		Object heartbeat_at = row.getData("heartbeat_at");
		Object old_heartbeat_at = row.getOldData("heartbeat_at"); // make sure it's a heartbeat update, not a position set.

		if ( heartbeat_at != null && old_heartbeat_at != null ) {
			Long thisHeartbeat = (Long) heartbeat_at;
			if ( !thisHeartbeat.equals(lastHeartbeatRead) ) {
				this.lastHeartbeatRead = thisHeartbeat;

				return HeartbeatRowMap.valueOf(row.getDatabase(), row.getPosition(), thisHeartbeat);
			}
		}
		return row;
	}

	/**
	 * Get the last heartbeat that the replicator has processed.
	 *
	 * We pass along the value of the heartbeat to the producer inside the row map.
	 * @return the millisecond value ot fhte last heartbeat
	 */

	public Long getLastHeartbeatRead() {
		return lastHeartbeatRead;
	}
}
