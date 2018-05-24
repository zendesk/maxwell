package com.zendesk.maxwell.recovery;

import com.zendesk.maxwell.replication.Position;

public class RecoveryInfo {
	public Position position;
	public Long serverID;
	public String clientID;

	public RecoveryInfo(Position position, Long serverID, String clientID) {
		this.position = position;
		this.serverID = serverID;
		this.clientID = clientID;
	}

	public long getHeartbeat() {
		return position.getLastHeartbeatRead();
	}

	public String toString() {
		return "<RecoveryInfo" +
				" position: " + position +
				", serverId: " + serverID +
				", clientId: " + clientID +
				">";
	}
}
