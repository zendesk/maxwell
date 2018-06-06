package com.zendesk.maxwell.core.config;

import com.zendesk.maxwell.core.producer.EncryptionMode;

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
