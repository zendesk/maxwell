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

	private final ArrayList<Pattern> excludeColumns = new ArrayList<>();

	private final HashMap<String, Integer> rowFilter = new HashMap<>();

	public MaxwellFilter() { }
	public MaxwellFilter(String includeDatabases,
						 String excludeDatabases,
						 String includeTables,
						 String excludeTables,
						 String blacklistDatabases,
						 String blacklistTables,
						 String excludeColumns) throws MaxwellInvalidFilterException {
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

		if ( excludeColumns != null ) {
			for (String s : excludeColumns.split(","))
				excludeColumns(s);
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

	public void excludeColumns(String name) throws MaxwellInvalidFilterException {
		excludeColumns.add(compile(name));
	}

	public void blacklistTable(String name) throws MaxwellInvalidFilterException {
		blacklistTables.add(compile(name));
	}

	private Pattern compile(String name) throws MaxwellInvalidFilterException {
		name = name.trim();
		if ( name.startsWith("/") ) {
			if ( !name.endsWith("/") ) {
				throw new MaxwellInvalidFilterException("Invalid regular expression: " + name);
			}
			return Pattern.compile(name.substring(1, name.length() - 1));
		} else {
			return Pattern.compile("^" + name + "$");
		}

	}

	public void addRowConstraint(String field, Integer value) {
		rowFilter.put(field, value);
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

	public boolean matchesRow(MaxwellAbstractRowsEvent e, Row r) {
		for (Map.Entry<String, Integer> entry : rowFilter.entrySet()) {
			Column c = e.findColumn(entry.getKey(), r);
			if ( c == null )
				return false;

			if ( c.getValue() == entry.getValue() ) {
				continue; // null or same instance
			}

			if ( c.getValue() == null || entry.getValue() == null ) {
				return false; // one side is null
			}

			if ( !c.getValue().equals(entry.getValue())) {
				return false;
			}

		}
		return true;
	}

	private boolean matchesAnyRows(MaxwellAbstractRowsEvent e) {
		for (Row r : e.getRows()) {
			if ( matchesRow(e, r) )
				return true;
		}
		return false;
	}

	public boolean matches(MaxwellAbstractRowsEvent e) {
		String database = e.getTable().getDatabase();
		String table = e.getTable().getName();
		return ( database.equals("maxwell") && table.equals("bootstrap") )
			|| ( matchesDatabase(database) && matchesTable(table) && matchesAnyRows(e) );
	}

	public boolean isDatabaseBlacklisted(String databaseName) {
		return ! matchesIncludeExcludeList(emptyList, blacklistDatabases, databaseName);
	}

	public boolean isTableBlacklisted(String databaseName, String tableName) {
		return isDatabaseBlacklisted(databaseName) ||
			   ! matchesIncludeExcludeList(emptyList, blacklistTables, tableName);
	}

	public boolean hasExcludeColumns() {
		return (excludeColumns.size() > 0);
	}

	public ArrayList<Pattern> getExcludeColumns() {
		return excludeColumns;
	}

	private void throwUnlessEmpty(HashSet<String> set, String objType) {
		if ( set.size() != 0 ) {
			throw new IllegalArgumentException("A " + objType + " filter may only be inclusive or exclusive");
		}
	}
}
