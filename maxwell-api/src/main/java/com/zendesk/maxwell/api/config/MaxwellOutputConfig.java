package com.zendesk.maxwell.api.config;

import com.zendesk.maxwell.api.producer.EncryptionMode;

import java.util.List;
import java.util.regex.Pattern;

public interface MaxwellOutputConfig {
	boolean DEFAULT_INCLUDE_BINLOG_POSITION = false;
	boolean DEFAULT_INCLUDE_GTID_POSITION = false;
	boolean DEFAULT_INCLUDE_COMMIT_INFO = true;
	boolean DEFAULT_INCLUDE_XOFFSET = true;
	boolean DEFAULT_INCLUDE_NULLS = true;
	boolean DEFAULT_INCLUDE_SERVER_ID = false;
	boolean DEFAULT_INCLUDE_THREAD_ID = false;
	boolean DEFAULT_INCLUDE_ROW_QUERY = false;
	boolean DEFAULT_OUTPUT_DDL = false;
	String DEFAULT_ENCRYPTION_MODE = "none";

	boolean isEncryptionEnabled();

	boolean isIncludesBinlogPosition();

	boolean isIncludesGtidPosition();

	boolean isIncludesCommitInfo();

	boolean isIncludesXOffset();

	boolean isIncludesNulls();

	boolean isIncludesServerId();

	boolean isIncludesThreadId();

	boolean isIncludesRowQuery();

	boolean isOutputDDL();

	List<Pattern> getExcludeColumns();

	EncryptionMode getEncryptionMode();

	String getSecretKey();
}
