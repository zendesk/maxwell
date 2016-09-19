package com.zendesk.maxwell.producer;

public class MaxwellOutputConfig {
	public boolean includesBinlogPosition;
	public boolean includesCommitInfo;
	public boolean omitNull;

	public MaxwellOutputConfig() {
		this.includesBinlogPosition = false;
		this.includesCommitInfo = true;
		this.omitNull = false;
	}
}
