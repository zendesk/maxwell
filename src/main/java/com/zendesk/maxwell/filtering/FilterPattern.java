package com.zendesk.maxwell.filtering;

import java.util.regex.Pattern;

public class FilterPattern {
	private final FilterPatternType type;
	private final Pattern dbPattern, tablePattern;

	public FilterPattern(FilterPatternType type, Pattern dbPattern, Pattern tablePattern) {
		this.type = type;
		this.dbPattern = dbPattern;
		this.tablePattern = tablePattern;
	}

	public void match(String database, String table, FilterResult match) {
		if ( (database == null || dbPattern.matcher(database).find())
		  && (table == null || tablePattern.matcher(table).find()) )
			match.include = (this.type == FilterPatternType.INCLUDE);
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

	private String patternToString(Pattern p) {
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
}
