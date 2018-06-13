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

	private boolean includesBinlogPosition;
	private boolean includesGtidPosition;
	private boolean includesCommitInfo;
	private boolean includesXOffset;
	private boolean includesNulls;
	private boolean includesServerId;
	private boolean includesThreadId;
	private boolean includesRowQuery;
	private boolean outputDDL;
	private List<Pattern> excludeColumns;
	private EncryptionMode encryptionMode;
	private String secretKey;

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
		return getEncryptionMode() != EncryptionMode.ENCRYPT_NONE;
	}

	public boolean isIncludesBinlogPosition() {
		return includesBinlogPosition;
	}

	public void setIncludesBinlogPosition(boolean includesBinlogPosition) {
		this.includesBinlogPosition = includesBinlogPosition;
	}

	public boolean isIncludesGtidPosition() {
		return includesGtidPosition;
	}

	public void setIncludesGtidPosition(boolean includesGtidPosition) {
		this.includesGtidPosition = includesGtidPosition;
	}

	public boolean isIncludesCommitInfo() {
		return includesCommitInfo;
	}

	public void setIncludesCommitInfo(boolean includesCommitInfo) {
		this.includesCommitInfo = includesCommitInfo;
	}

	public boolean isIncludesXOffset() {
		return includesXOffset;
	}

	public void setIncludesXOffset(boolean includesXOffset) {
		this.includesXOffset = includesXOffset;
	}

	public boolean isIncludesNulls() {
		return includesNulls;
	}

	public void setIncludesNulls(boolean includesNulls) {
		this.includesNulls = includesNulls;
	}

	public boolean isIncludesServerId() {
		return includesServerId;
	}

	public void setIncludesServerId(boolean includesServerId) {
		this.includesServerId = includesServerId;
	}

	public boolean isIncludesThreadId() {
		return includesThreadId;
	}

	public void setIncludesThreadId(boolean includesThreadId) {
		this.includesThreadId = includesThreadId;
	}

	public boolean isIncludesRowQuery() {
		return includesRowQuery;
	}

	public void setIncludesRowQuery(boolean includesRowQuery) {
		this.includesRowQuery = includesRowQuery;
	}

	public boolean isOutputDDL() {
		return outputDDL;
	}

	public void setOutputDDL(boolean outputDDL) {
		this.outputDDL = outputDDL;
	}

	public List<Pattern> getExcludeColumns() {
		return excludeColumns;
	}

	public void setExcludeColumns(List<Pattern> excludeColumns) {
		this.excludeColumns = excludeColumns;
	}

	public EncryptionMode getEncryptionMode() {
		return encryptionMode;
	}

	public void setEncryptionMode(EncryptionMode encryptionMode) {
		this.encryptionMode = encryptionMode;
	}

	public String getSecretKey() {
		return secretKey;
	}

	public void setSecretKey(String secretKey) {
		this.secretKey = secretKey;
	}
}
