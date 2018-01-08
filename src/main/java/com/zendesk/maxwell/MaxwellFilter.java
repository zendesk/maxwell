package com.zendesk.maxwell;

import org.apache.commons.lang.StringUtils;

import java.util.*;
import java.util.regex.Pattern;

/*
	filters compile down to:
	(includeDatabases.nil? || includeDatabases.include?()
 */
public class MaxwellFilter {
	private static final List<Pattern> emptyList = Collections.unmodifiableList(new ArrayList<Pattern>());
	private final ArrayList<Pattern> includeDatabases = new ArrayList<>();
	private final ArrayList<Pattern> excludeDatabases = new ArrayList<>();
	private final ArrayList<Pattern> includeTables = new ArrayList<>();
	private final ArrayList<Pattern> excludeTables = new ArrayList<>();
	private final ArrayList<Pattern> blacklistDatabases = new ArrayList<>();
	private final ArrayList<Pattern> blacklistTables = new ArrayList<>();
	private final Map<String, String> includeColumnValues = new HashMap<>();

	public MaxwellFilter() { }

	public MaxwellFilter(
		String includeDatabases,
		String excludeDatabases,
		String includeTables,
		String excludeTables,
		String blacklistDatabases,
		String blacklistTables,
		String includeColumnValues
	) throws MaxwellInvalidFilterException {
		this(includeDatabases, excludeDatabases, includeTables, excludeTables, blacklistDatabases, blacklistTables);

		if (includeColumnValues != null && !"".equals(includeColumnValues)) {
			for (String s : includeColumnValues.split(",")) {
				String[] columnAndValue = s.split("=");
				includeColumnValue(columnAndValue[0], columnAndValue[1]);
			}
		}
	}

	public MaxwellFilter(
		String includeDatabases,
		String excludeDatabases,
		String includeTables,
		String excludeTables,
		String blacklistDatabases,
		String blacklistTables
	) throws MaxwellInvalidFilterException {
		if ( includeDatabases != null ) {
			for ( String s : includeDatabases.split(",") )
				includeDatabase(s);
		}

		if ( excludeDatabases != null ) {
			for ( String s : excludeDatabases.split(",") )
				excludeDatabase(s);
		}

		if ( includeTables != null ) {
			for ( String s : includeTables.split(",") )
				includeTable(s);
		}

		if ( excludeTables != null ) {
			for ( String s : excludeTables.split(",") )
				excludeTable(s);
		}

		if ( blacklistDatabases != null ) {
			for ( String s : blacklistDatabases.split(",") )
				blacklistDatabases(s);
		}

		if ( blacklistTables != null ) {
			for ( String s : blacklistTables.split(",") )
				blacklistTable(s);
		}
	}

	public void includeDatabase(String name) throws MaxwellInvalidFilterException {
		includeDatabases.add(compile(name));
	}

	public void excludeDatabase(String name) throws MaxwellInvalidFilterException {
		excludeDatabases.add(compile(name));
	}

	public void includeTable(String name) throws MaxwellInvalidFilterException {
		includeTables.add(compile(name));
	}

	public void excludeTable(String name) throws MaxwellInvalidFilterException {
		excludeTables.add(compile(name));
	}

	public void blacklistDatabases(String name) throws MaxwellInvalidFilterException {
		blacklistDatabases.add(compile(name));
	}

	public void blacklistTable(String name) throws MaxwellInvalidFilterException {
		blacklistTables.add(compile(name));
	}

	public void includeColumnValue(String column, String value) throws MaxwellInvalidFilterException {
		includeColumnValues.put(column, value);
	}

	public boolean isDatabaseWhitelist() {
		return !includeDatabases.isEmpty();
	}

	public boolean isTableWhitelist() {
		return !includeTables.isEmpty();
	}

	private Pattern compile(String name) throws MaxwellInvalidFilterException {
		return MaxwellConfig.compileStringToPattern(name);
	}

	private boolean filterListsInclude(List<Pattern> includeList, List<Pattern> excludeList, String name) {
		if ( includeList.size() > 0 ) {
			boolean found = false;
			for ( Pattern p : includeList ) {
				found = p.matcher(name).find();

				if ( found )
					break;
			}
			if ( !found )
				return false;
		}

		for ( Pattern p : excludeList ) {
			if ( p.matcher(name).find() )
				return false;
		}

		return true;
	}

	private boolean matchesDatabase(String dbName) {
		return filterListsInclude(includeDatabases, excludeDatabases, dbName);
	}

	private boolean matchesTable(String tableName) {
		return filterListsInclude(includeTables, excludeTables, tableName);
	}

	public boolean matches(String database, String table) {
		if (table == null) {
			return matchesDatabase(database);
		}
		return matchesDatabase(database) && matchesTable(table);
	}

	private boolean matchesValues(Map<String, Object> data) {
		for (Map.Entry<String, String> entry : includeColumnValues.entrySet()) {
			String column = entry.getKey();

			if (data.containsKey(column)) {
				String expectedColumnValue = entry.getValue();
				Object value = data.get(column);

				if ("NULL".equals(expectedColumnValue)) {
					// null or "null" (string) or "NULL" (string) is expected
					if (value != null && !"null".equals(value) && !"NULL".equals(value)) {
						return false;
					}
				} else {
					if (value == null || !expectedColumnValue.equals(value.toString())) {
						return false;
					}
				}
			}
		}

		return true;
	}

	public boolean isDatabaseBlacklisted(String databaseName) {
		return ! filterListsInclude(emptyList, blacklistDatabases, databaseName);
	}

	public boolean isTableBlacklisted(String databaseName, String tableName) {
		return isSystemBlacklisted(databaseName, tableName)
			|| isDatabaseBlacklisted(databaseName)
			|| !filterListsInclude(emptyList, blacklistTables, tableName);
	}

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

	public static boolean matchesValues(MaxwellFilter filter, String database, String table, Map<String, Object> data) {
		if (filter == null) {
			return true;
		} else {
			return filter.matchesValues(data);
		}
	}
}
