package com.zendesk.maxwell.producer;

public class MaxwellOutputConfig {
	public boolean includesBinlogPosition;
	public boolean includesCommitInfo;
	public boolean includesNulls;

	public MaxwellOutputConfig() {
		this.includesBinlogPosition = false;
		this.includesCommitInfo = true;
		this.includesNulls = true;
	}
}
