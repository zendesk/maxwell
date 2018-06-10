package com.zendesk.maxwell.api.config;

import java.util.Map;

public interface MaxwellFilter {
	String CONFIGURATION_OPTION_INCLUDE_DBS = "include_dbs";
	String CONFIGURATION_OPTION_EXCLUDE_DBS = "exclude_dbs";
	String CONFIGURATION_OPTION_INCLUDE_TABLES = "include_tables";
	String CONFIGURATION_OPTION_EXCLUDE_TABLES = "exclude_tables";
	String CONFIGURATION_OPTION_BLACKLIST_DBS = "blacklist_dbs";
	String CONFIGURATION_OPTION_BLACKLIST_TABLES = "blacklist_tables";
	String CONFIGURATION_OPTION_INCLUDE_COLUMN_VALUES = "include_column_values";

	void includeDatabase(String name) throws MaxwellInvalidFilterException;

	void excludeDatabase(String name) throws MaxwellInvalidFilterException;

	void includeTable(String name) throws MaxwellInvalidFilterException;

	void excludeTable(String name) throws MaxwellInvalidFilterException;

	void blacklistDatabases(String name) throws MaxwellInvalidFilterException;

	void blacklistTable(String name) throws MaxwellInvalidFilterException;

	void includeColumnValue(String column, String value) throws MaxwellInvalidFilterException;

	boolean isDatabaseWhitelist();

	boolean isTableWhitelist();

	boolean matches(String database, String table);

	boolean matchesValues(Map<String, Object> data);

	boolean isDatabaseBlacklisted(String databaseName);

	boolean isTableBlacklisted(String databaseName, String tableName);
}
