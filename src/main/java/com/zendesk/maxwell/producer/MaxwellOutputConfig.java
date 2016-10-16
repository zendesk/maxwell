package com.zendesk.maxwell.producer;

public class MaxwellOutputConfig {
	public boolean includesBinlogPosition;
	public boolean includesCommitInfo;
	public boolean includesNulls;
	public boolean includesServerId;
	public boolean includesThreadId;
	public boolean outputDDL;

	public MaxwellOutputConfig() {
		this.includesBinlogPosition = false;
		this.includesCommitInfo = true;
		this.includesNulls = true;
		this.includesServerId = false;
		this.includesThreadId = false;
		this.outputDDL = false;
	}
}
