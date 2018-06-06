package com.zendesk.maxwell.api.config;

import com.zendesk.maxwell.api.producer.EncryptionMode;

import java.util.List;
import java.util.regex.Pattern;

public interface MaxwellOutputConfig {
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
