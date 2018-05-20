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
		if ( (dbPattern == null || database == null || dbPattern.matcher(database).find())
		  && (tablePattern == null || table == null || tablePattern.matcher(table).find()) )
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
}
