package com.zendesk.maxwell.core.config;

import com.zendesk.maxwell.api.config.MaxwellFilter;
import org.apache.commons.lang.StringUtils;

import java.util.Map;

public class MaxwellFilterSupport {
	public static boolean isSystemBlacklisted(String databaseName, String tableName) {
		return "mysql".equals(databaseName) &&
				("ha_health_check".equals(tableName) || StringUtils.startsWith(tableName, "rds_heartbeat"));
	}

	public static boolean matches(MaxwellFilter filter, String database, String table) {
		if (filter == null) {
			return true;
		} else {
			return filter.matches(database, table);
		}
	}

	public static boolean matchesValues(MaxwellFilter filter, Map<String, Object> data) {
		if (filter == null) {
			return true;
		} else {
			return filter.matchesValues(data);
		}
	}
}
