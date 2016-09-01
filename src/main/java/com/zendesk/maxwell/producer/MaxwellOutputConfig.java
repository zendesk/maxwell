package com.zendesk.maxwell.producer;

public class MaxwellOutputConfig {
	public final boolean includesBinlogPosition;
	public final boolean includesCommitInfo;

	public MaxwellOutputConfig(boolean includesBinlogPosition, boolean includesCommitInfo) {
		this.includesBinlogPosition = includesBinlogPosition;
		this.includesCommitInfo = includesCommitInfo;
	}
}
