package com.zendesk.maxwell.core.config;

import com.zendesk.maxwell.api.config.MaxwellFilter;
import com.zendesk.maxwell.api.config.MaxwellInvalidFilterException;

import java.util.*;
import java.util.regex.Pattern;

/*
	filters compile down to:
	(includeDatabases.nil? || includeDatabases.include?()
 */
public class BaseMaxwellFilter implements MaxwellFilter {
	private static final List<Pattern> emptyList = Collections.unmodifiableList(new ArrayList<Pattern>());
	private final ArrayList<Pattern> includeDatabases = new ArrayList<>();
	private final ArrayList<Pattern> excludeDatabases = new ArrayList<>();
	private final ArrayList<Pattern> includeTables = new ArrayList<>();
	private final ArrayList<Pattern> excludeTables = new ArrayList<>();
	private final ArrayList<Pattern> blacklistDatabases = new ArrayList<>();
	private final ArrayList<Pattern> blacklistTables = new ArrayList<>();
	private final Map<String, String> includeColumnValues = new HashMap<>();

	public BaseMaxwellFilter() { }

	public BaseMaxwellFilter(
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

	public BaseMaxwellFilter(
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

	@Override
	public void includeDatabase(String name) throws MaxwellInvalidFilterException {
		includeDatabases.add(compile(name));
	}

	@Override
	public void excludeDatabase(String name) throws MaxwellInvalidFilterException {
		excludeDatabases.add(compile(name));
	}

	@Override
	public void includeTable(String name) throws MaxwellInvalidFilterException {
		includeTables.add(compile(name));
	}

	@Override
	public void excludeTable(String name) throws MaxwellInvalidFilterException {
		excludeTables.add(compile(name));
	}

	@Override
	public void blacklistDatabases(String name) throws MaxwellInvalidFilterException {
		blacklistDatabases.add(compile(name));
	}

	@Override
	public void blacklistTable(String name) throws MaxwellInvalidFilterException {
		blacklistTables.add(compile(name));
	}

	@Override
	public void includeColumnValue(String column, String value) throws MaxwellInvalidFilterException {
		includeColumnValues.put(column, value);
	}

	@Override
	public boolean isDatabaseWhitelist() {
		return !includeDatabases.isEmpty();
	}

	@Override
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

	@Override
	public boolean matches(String database, String table) {
		if (table == null) {
			return matchesDatabase(database);
		}
		return matchesDatabase(database) && matchesTable(table);
	}

	@Override
	public boolean matchesValues(Map<String, Object> data) {
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

	@Override
	public boolean isDatabaseBlacklisted(String databaseName) {
		return ! filterListsInclude(emptyList, blacklistDatabases, databaseName);
	}

	@Override
	public boolean isTableBlacklisted(String databaseName, String tableName) {
		return MaxwellFilterSupport.isSystemBlacklisted(databaseName, tableName)
			|| isDatabaseBlacklisted(databaseName)
			|| !filterListsInclude(emptyList, blacklistTables, tableName);
	}

}
