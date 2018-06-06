package com.zendesk.maxwell.core.config;

import com.zendesk.maxwell.core.MaxwellInvalidFilterException;
import org.apache.commons.lang.StringUtils;

import java.util.Map;

public interface MaxwellFilter {
	static boolean isSystemBlacklisted(String databaseName, String tableName) {
		return "mysql".equals(databaseName) &&
			("ha_health_check".equals(tableName) || StringUtils.startsWith(tableName, "rds_heartbeat"));
	}

	static boolean matches(MaxwellFilter filter, String database, String table) {
		if (filter == null) {
			return true;
		} else {
			return filter.matches(database, table);
		}
	}

	static boolean matchesValues(MaxwellFilter filter, Map<String, Object> data) {
		if (filter == null) {
			return true;
		} else {
			return filter.matchesValues(data);
		}
	}

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
