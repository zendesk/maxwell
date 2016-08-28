package com.zendesk.maxwell;

public class MaxwellMasterRecoveryInfo {
	public BinlogPosition position;
	public Long heartbeat;
	public Long serverID;

	public MaxwellMasterRecoveryInfo(BinlogPosition position, Long heartbeat, Long serverID) {
		this.position = position;
		this.heartbeat = heartbeat;
		this.serverID = serverID;
	}
}
