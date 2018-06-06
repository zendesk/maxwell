package com.zendesk.maxwell.core.config;

import com.zendesk.maxwell.core.producer.EncryptionMode;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class BaseMaxwellOutputConfig implements MaxwellOutputConfig {
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

	public BaseMaxwellOutputConfig() {
		includesBinlogPosition = false;
		includesGtidPosition = false;
		includesCommitInfo = true;
		includesNulls = true;
		includesServerId = false;
		includesThreadId = false;
		includesRowQuery = false;
		outputDDL = false;
		excludeColumns = new ArrayList<>();
		encryptionMode = EncryptionMode.ENCRYPT_NONE;
	}

	@Override
	public boolean isEncryptionEnabled() {
		return getEncryptionMode() != EncryptionMode.ENCRYPT_NONE;
	}

	@Override
	public boolean isIncludesBinlogPosition() {
		return includesBinlogPosition;
	}

	public void setIncludesBinlogPosition(boolean includesBinlogPosition) {
		this.includesBinlogPosition = includesBinlogPosition;
	}

	@Override
	public boolean isIncludesGtidPosition() {
		return includesGtidPosition;
	}

	public void setIncludesGtidPosition(boolean includesGtidPosition) {
		this.includesGtidPosition = includesGtidPosition;
	}

	@Override
	public boolean isIncludesCommitInfo() {
		return includesCommitInfo;
	}

	public void setIncludesCommitInfo(boolean includesCommitInfo) {
		this.includesCommitInfo = includesCommitInfo;
	}

	@Override
	public boolean isIncludesXOffset() {
		return includesXOffset;
	}

	public void setIncludesXOffset(boolean includesXOffset) {
		this.includesXOffset = includesXOffset;
	}

	@Override
	public boolean isIncludesNulls() {
		return includesNulls;
	}

	public void setIncludesNulls(boolean includesNulls) {
		this.includesNulls = includesNulls;
	}

	@Override
	public boolean isIncludesServerId() {
		return includesServerId;
	}

	public void setIncludesServerId(boolean includesServerId) {
		this.includesServerId = includesServerId;
	}

	@Override
	public boolean isIncludesThreadId() {
		return includesThreadId;
	}

	public void setIncludesThreadId(boolean includesThreadId) {
		this.includesThreadId = includesThreadId;
	}

	@Override
	public boolean isIncludesRowQuery() {
		return includesRowQuery;
	}

	public void setIncludesRowQuery(boolean includesRowQuery) {
		this.includesRowQuery = includesRowQuery;
	}

	@Override
	public boolean isOutputDDL() {
		return outputDDL;
	}

	public void setOutputDDL(boolean outputDDL) {
		this.outputDDL = outputDDL;
	}

	@Override
	public List<Pattern> getExcludeColumns() {
		return excludeColumns;
	}

	public void setExcludeColumns(List<Pattern> excludeColumns) {
		this.excludeColumns = excludeColumns;
	}

	@Override
	public EncryptionMode getEncryptionMode() {
		return encryptionMode;
	}

	public void setEncryptionMode(EncryptionMode encryptionMode) {
		this.encryptionMode = encryptionMode;
	}

	@Override
	public String getSecretKey() {
		return secretKey;
	}

	public void setSecretKey(String secretKey) {
		this.secretKey = secretKey;
	}
}
