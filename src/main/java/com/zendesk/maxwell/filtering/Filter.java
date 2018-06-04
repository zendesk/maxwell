package com.zendesk.maxwell.filtering;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class Filter {
	static final Logger LOGGER = LoggerFactory.getLogger(Filter.class);

	private final List<FilterPattern> patterns;
	private final Map<String, String> includeColumnValues = new HashMap<>();

	public Filter() {
		this.patterns = new ArrayList<>();
	}

	public Filter(String filterString, String valueString) throws InvalidFilterException {
		this();

		if (valueString != null && !"".equals(valueString)) {
			for (String s : valueString.split(",")) {
				String[] columnAndValue = s.split("=");
				includeColumnValues.put(columnAndValue[0], columnAndValue[1]);
			}
		}

		patterns.addAll(new FilterParser(filterString).parse());
	}

	public void addRule(String filterString) throws InvalidFilterException {
		this.patterns.addAll(new FilterParser(filterString).parse());
	}

	public List<FilterPattern> getRules() {
		return new ArrayList<>(this.patterns);
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

	public boolean includes(String database, String table) {
		FilterResult match = new FilterResult();

		for ( FilterPattern p : patterns )
			p.match(database, table, match);

		return match.include;
	}

	public boolean isTableBlacklisted(String database, String table) {
		FilterResult match = new FilterResult();

		for ( FilterPattern p : patterns ) {
			if ( p.getType() == FilterPatternType.BLACKLIST )
				p.match(database, table, match);
		}

		return !match.include;
	}

	public boolean isDatabaseBlacklisted(String database) {
		for ( FilterPattern p : patterns ) {
			if (p.getType() == FilterPatternType.BLACKLIST &&
				p.getDatabasePattern().matcher(database).find() &&
				p.getTablePattern().toString().equals(""))
				return true;
		}

		return false;
	}

	public static boolean isSystemBlacklisted(String databaseName, String tableName) {
		return "mysql".equals(databaseName) &&
			("ha_health_check".equals(tableName) || StringUtils.startsWith(tableName, "rds_heartbeat"));
	}

	public static boolean includes(Filter filter, String database, String table) {
		if (filter == null) {
			return true;
		} else {
			return filter.includes(database, table);
		}
	}

	public static boolean matchesValues(Filter filter, Map<String, Object> data) {
		if (filter == null) {
			return true;
		} else {
			return filter.matchesValues(data);
		}
	}

	public static Filter fromOldFormat(
		String includeDatabases,
		String excludeDatabases,
		String includeTables,
		String excludeTables,
		String blacklistDatabases,
		String blacklistTables,
		String includeValues
	) throws InvalidFilterException {
		ArrayList<String> filterRules = new ArrayList<>();

		if ( blacklistDatabases != null ) {
			for ( String s : blacklistDatabases.split(",") )
				filterRules.add("blacklist: " + s + ".*");
		}

		if ( blacklistTables != null ) {
			for (String s : blacklistTables.split(","))
				filterRules.add("blacklist: *." + s);
		}

		/* any include in old-filters is actually exclude *.* */
		if ( includeDatabases != null || includeTables != null ) {
			filterRules.add("exclude: *.*");
		}

		if ( includeDatabases != null ) {
			for ( String s : includeDatabases.split(",") )
				filterRules.add("include: " + s + ".*");

		}

		if ( excludeDatabases != null ) {
			for (String s : excludeDatabases.split(","))
				filterRules.add("exclude: " + s + ".*");
		}

		if ( includeTables != null ) {
			for ( String s : includeTables.split(",") )
				filterRules.add("include: *." + s);
		}

		if ( excludeTables != null ) {
			for ( String s : excludeTables.split(",") )
				filterRules.add("exclude: *." + s);
		}

		String filterRulesAsString = String.join(", ", filterRules);
		LOGGER.warn("using exclude/include is deprecated.  Please update your configuration to use: ");
		LOGGER.warn("filter = \"" + filterRulesAsString + "\"");

		return new Filter(filterRulesAsString, includeValues);
	}
}
