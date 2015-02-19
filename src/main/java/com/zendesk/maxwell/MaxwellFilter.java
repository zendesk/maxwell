package com.zendesk.maxwell;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import com.google.code.or.common.glossary.Column;
import com.google.code.or.common.glossary.Row;

public class MaxwellFilter {
	private final HashSet<String> includeDatabases = new HashSet<>();
	private final HashSet<String> excludeDatabases = new HashSet<>();
	private final HashSet<String> includeTables    = new HashSet<>();
	private final HashSet<String> excludeTables    = new HashSet<>();
	private final HashMap<String, Integer> rowFilter = new HashMap<>();

	public void includeDatabase(String dbName) {
		throwUnlessEmpty(excludeDatabases, "database");
		includeDatabases.add(dbName);
	}

	public void excludeDatabase(String dbName) {
		throwUnlessEmpty(includeDatabases, "database");
		excludeDatabases.add(dbName);
	}

	public void includeTable(String tblName) {
		throwUnlessEmpty(excludeTables, "table");
		includeTables.add(tblName);
	}

	public void excludeTable(String tblName) {
		throwUnlessEmpty(includeTables, "table");
		excludeTables.add(tblName);
	}

	public void addRowConstraint(String field, Integer value) {
		rowFilter.put(field, value);
	}

	private boolean matchesDatabase(String dbName) {
		if ( includeDatabases.size() > 0 ) {
			return includeDatabases.contains(dbName);
		} else {
			return !excludeDatabases.contains(dbName);
		}
	}

	private boolean matchesTable(String tblName) {
		if ( includeTables.size() > 0 ) {
			return includeTables.contains(tblName);
		} else {
			return !excludeTables.contains(tblName);
		}
	}

	public boolean matchesRow(MaxwellAbstractRowsEvent e, Row r) {
		for (Map.Entry<String, Integer> entry : rowFilter.entrySet()) {
			Column c = e.findColumn(entry.getKey(), r);
			if ( c == null )
				return false;

			if ( (Integer) c.getValue() != entry.getValue() ) {
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
		return matchesDatabase(e.getTable().getDatabase().getName())
		    && matchesTable(e.getTable().getName())
		    && matchesAnyRows(e);
	}


	private void throwUnlessEmpty(HashSet<String> set, String objType) {
		if ( set.size() != 0 ) {
			throw new IllegalArgumentException("A " + objType + " filter may only be inclusive or exclusive");
		}
	}
}
