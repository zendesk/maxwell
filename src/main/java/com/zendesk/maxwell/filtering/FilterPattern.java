package com.zendesk.maxwell.filtering;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public class FilterPattern {
	protected final FilterPatternType type;
	private final Pattern dbPattern, tablePattern;

	public FilterPattern(FilterPatternType type, Pattern dbPattern, Pattern tablePattern) {
		this.type = type;
		this.dbPattern = dbPattern;
		this.tablePattern = tablePattern;
	}

	protected boolean appliesTo(String database, String table) {
		return (database == null || dbPattern.matcher(database).find())
			&& (table == null || tablePattern.matcher(table).find());
	}
	public void match(String database, String table, FilterResult match) {
		if ( appliesTo(database, table) )
			match.include = (this.type == FilterPatternType.INCLUDE);
	}

	public void matchValue(String database, String table, Map<String, Object> data, FilterResult match) {
		match(database, table, match);
	}

	public FilterPatternType getType() {
		return type;
	}

	public Pattern getDatabasePattern() {
		return dbPattern;
	}

	public Pattern getTablePattern() {
		return tablePattern;
	}

	protected String patternToString(Pattern p) {
		String s = p.pattern();

		if ( s.equals("") ) {
			return "*";
		} else if ( s.startsWith("^") && s.endsWith("$") ) {
			return s.substring(1, s.length() - 1);
		} else {
			return "/" + s + "/";
		}
	}

	@Override
	public String toString() {
		String s = "";
		switch ( type ) {
			case INCLUDE:
				s += "include: ";
				break;
			case EXCLUDE:
				s += "exclude: ";
				break;
			case BLACKLIST:
				s += "blacklist: ";
				break;
		}

		return s + patternToString(dbPattern) + "." + patternToString(tablePattern);
	}

	public boolean couldIncludeColumn(String database, String table, Set<String> columns) {
		return false;
	}
}
