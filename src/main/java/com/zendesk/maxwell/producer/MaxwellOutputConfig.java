package com.zendesk.maxwell.producer;

import java.util.List;
import java.util.ArrayList;
import java.util.regex.Pattern;

public class MaxwellOutputConfig {
	public boolean includesBinlogPosition;
	public boolean includesCommitInfo;
	public boolean includesNulls;
	public boolean includesServerId;
	public boolean includesThreadId;
	public boolean outputDDL;
	public List<Pattern> excludeColumns;

	public MaxwellOutputConfig() {
		this.includesBinlogPosition = false;
		this.includesCommitInfo = true;
		this.includesNulls = true;
		this.includesServerId = false;
		this.includesThreadId = false;
		this.outputDDL = false;
		this.excludeColumns = new ArrayList<>();
	}
}
