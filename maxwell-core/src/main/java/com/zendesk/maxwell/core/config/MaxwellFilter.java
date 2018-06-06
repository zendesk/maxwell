package com.zendesk.maxwell.core.config;

import com.zendesk.maxwell.core.MaxwellInvalidFilterException;

import java.util.Map;

public interface MaxwellFilter {
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
