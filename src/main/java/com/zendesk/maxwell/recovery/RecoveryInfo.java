package com.zendesk.maxwell.recovery;

import com.zendesk.maxwell.replication.BinlogPosition;

public class RecoveryInfo {
	public BinlogPosition position;
	public Long heartbeat;
	public Long serverID;
	public String clientID;

	public RecoveryInfo(BinlogPosition position, Long heartbeat, Long serverID, String clientID) {
		this.position = position;
		this.heartbeat = heartbeat;
		this.serverID = serverID;
		this.clientID = clientID;
	}

	public String toString() {
		return "<RecoveryInfo" +
				" position: " + position +
				", heartbeat: " + heartbeat +
				", serverId: " + serverID +
				", clientId: " + clientID +
				">";
	}
}
