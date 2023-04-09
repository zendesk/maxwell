package com.zendesk.maxwell.producer;

import java.util.List;
import java.util.ArrayList;
import java.util.regex.Pattern;


public class MaxwellOutputConfig {
	public boolean includesBinlogPosition;
	public boolean includesGtidPosition;
	public boolean includesCommitInfo;
	public boolean includesXOffset;
	public boolean includesNulls;
	public boolean includesServerId;
	public boolean includesThreadId;
	public boolean includesSchemaId;
	public boolean includesRowQuery;
	public boolean includesPrimaryKeys;
	public boolean includesPrimaryKeyColumns;
	public boolean includesPushTimestamp;
	public boolean outputDDL;
	public List<Pattern> excludeColumns;
	public EncryptionMode encryptionMode;
	public String secretKey;
	public boolean zeroDatesAsNull;
	public String namingStrategy;
	public int rowQueryMaxLength;
	
	public MaxwellOutputConfig() {
		this.includesBinlogPosition = false;
		this.includesGtidPosition = false;
		this.includesCommitInfo = true;
		this.includesNulls = true;
		this.includesServerId = false;
		this.includesThreadId = false;
		this.includesSchemaId = false;
		this.includesRowQuery = false;
		this.includesPrimaryKeys = false;
		this.includesPrimaryKeyColumns = false;
		this.includesPushTimestamp = false;
		this.outputDDL = false;
		this.zeroDatesAsNull = false;
		this.excludeColumns = new ArrayList<>();
		this.encryptionMode = EncryptionMode.ENCRYPT_NONE;
		this.secretKey = null;
		this.namingStrategy = null;
		this.rowQueryMaxLength = 0;
	}

	public boolean encryptionEnabled() {
		return encryptionMode != EncryptionMode.ENCRYPT_NONE;
	}
}
