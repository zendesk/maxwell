package com.zendesk.maxwell.core.config;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class MaxwellOutputConfig {
	public static final boolean DEFAULT_INCLUDE_BINLOG_POSITION = false;
	public static final boolean DEFAULT_INCLUDE_GTID_POSITION = false;
	public static final boolean DEFAULT_INCLUDE_COMMIT_INFO = true;
	public static final boolean DEFAULT_INCLUDE_XOFFSET = true;
	public static final boolean DEFAULT_INCLUDE_NULLS = true;
	public static final boolean DEFAULT_INCLUDE_SERVER_ID = false;
	public static final boolean DEFAULT_INCLUDE_THREAD_ID = false;
	public static final boolean DEFAULT_INCLUDE_ROW_QUERY = false;
	public static final boolean DEFAULT_OUTPUT_DDL = false;
	public static final String DEFAULT_ENCRYPTION_MODE = "none";

	public boolean includesBinlogPosition;
	public boolean includesGtidPosition;
	public boolean includesCommitInfo;
	public boolean includesXOffset;
	public boolean includesNulls;
	public boolean includesServerId;
	public boolean includesThreadId;
	public boolean includesRowQuery;
	public boolean outputDDL;
	public List<Pattern> excludeColumns;
	public EncryptionMode encryptionMode;
	public String secretKey;

	public MaxwellOutputConfig() {
		includesBinlogPosition = DEFAULT_INCLUDE_BINLOG_POSITION;
		includesGtidPosition = DEFAULT_INCLUDE_GTID_POSITION;
		includesCommitInfo = DEFAULT_INCLUDE_COMMIT_INFO;
		includesXOffset = DEFAULT_INCLUDE_XOFFSET;
		includesNulls = DEFAULT_INCLUDE_NULLS;
		includesServerId = DEFAULT_INCLUDE_SERVER_ID;
		includesThreadId = DEFAULT_INCLUDE_THREAD_ID;
		includesRowQuery = DEFAULT_INCLUDE_ROW_QUERY;
		outputDDL = DEFAULT_OUTPUT_DDL;
		excludeColumns = new ArrayList<>();
		encryptionMode = EncryptionMode.ENCRYPT_NONE;
	}

	public boolean isEncryptionEnabled() {
		return encryptionMode != null && encryptionMode != EncryptionMode.ENCRYPT_NONE;
	}

}
