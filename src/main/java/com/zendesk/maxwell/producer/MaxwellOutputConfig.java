package com.zendesk.maxwell.producer;

public class MaxwellOutputConfig {
	public final boolean includesBinlogPosition;
	public final boolean includesCommit;
	public final boolean includesXid;

	public MaxwellOutputConfig(boolean includesBinlogPosition, boolean includesCommit, boolean includesXid) {
		this.includesBinlogPosition = includesBinlogPosition;
		this.includesCommit = includesCommit;
		this.includesXid = includesXid;
	}
}
