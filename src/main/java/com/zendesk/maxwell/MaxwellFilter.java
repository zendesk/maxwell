package com.zendesk.maxwell;

import java.util.*;
import java.util.regex.Pattern;

import com.google.code.or.common.glossary.Column;
import com.google.code.or.common.glossary.Row;

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

	public MaxwellFilter() { }
	public MaxwellFilter(String includeDatabases,
						 String excludeDatabases,
						 String includeTables,
						 String excludeTables,
						 String blacklistDatabases,
						 String blacklistTables) throws MaxwellInvalidFilterException {
		if ( includeDatabases != null ) {
			for (String s : includeDatabases.split(","))
				includeDatabase(s);
		}

		if ( excludeDatabases != null ) {
			for (String s : excludeDatabases.split(","))
				excludeDatabase(s);
		}

		if ( includeTables != null ) {
			for ( String s : includeTables.split(",") )
				includeTable(s);
		}

		if ( excludeTables != null ) {
			for (String s : excludeTables.split(","))
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

	public boolean isDatabaseWhitelist() {
		return !includeDatabases.isEmpty();
	}

	public boolean isTableWhitelist() {
		return !includeTables.isEmpty();
	}

	private Pattern compile(String name) throws MaxwellInvalidFilterException {
		return MaxwellConfig.compileStringToPattern(name);
	}

	private boolean matchesIncludeExcludeList(List<Pattern> includeList, List<Pattern> excludeList, String name) {
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
		return matchesIncludeExcludeList(includeDatabases, excludeDatabases, dbName);
	}

	private boolean matchesTable(String tableName) {
		return matchesIncludeExcludeList(includeTables, excludeTables, tableName);
	}

	public boolean matches(String database, String table) {
		return matchesDatabase(database) && matchesTable(table);
	}

	public boolean isDatabaseBlacklisted(String databaseName) {
		return ! matchesIncludeExcludeList(emptyList, blacklistDatabases, databaseName);
	}

	public boolean isTableBlacklisted(String databaseName, String tableName) {
		return isDatabaseBlacklisted(databaseName) ||
			   ! matchesIncludeExcludeList(emptyList, blacklistTables, tableName);
	}
}
