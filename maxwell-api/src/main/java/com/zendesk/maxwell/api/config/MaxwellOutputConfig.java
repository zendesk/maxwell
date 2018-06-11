package com.zendesk.maxwell.api.config;

import com.zendesk.maxwell.api.producer.EncryptionMode;

import java.util.List;
import java.util.regex.Pattern;

public interface MaxwellOutputConfig {
	String CONFIGURATION_OPTION_OUTPUT_BINLOG_POSITION = "output_binlog_position";
	String CONFIGURATION_OPTION_OUTPUT_GTID_POSITION = "output_gtid_position";
	String CONFIGURATION_OPTION_OUTPUT_COMMIT_INFO = "output_commit_info";
	String CONFIGURATION_OPTION_OUTPUT_XOFFSET = "output_xoffset";
	String CONFIGURATION_OPTION_OUTPUT_NULLS = "output_nulls";
	String CONFIGURATION_OPTION_OUTPUT_SERVER_ID = "output_server_id";
	String CONFIGURATION_OPTION_OUTPUT_THREAD_ID = "output_thread_id";
	String CONFIGURATION_OPTION_OUTPUT_ROW_QUERY = "output_row_query";
	String CONFIGURATION_OPTION_OUTPUT_DDL = "output_ddl";
	String CONFIGURATION_OPTION_ENCRYPT = "encrypt";
	String CONFIGURATION_OPTION_SECRET_KEY = "secret_key";
	String CONFIGURATION_OPTION_EXCLUDE_COLUMNS = "exclude_columns";

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
