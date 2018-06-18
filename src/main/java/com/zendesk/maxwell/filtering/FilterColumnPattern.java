package com.zendesk.maxwell.filtering;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public class FilterColumnPattern extends FilterPattern {
	private static final Logger LOGGER = LoggerFactory.getLogger(FilterColumnPattern.class);
	private final String columnName;
	private final Pattern columnPattern;
	private final boolean columnPatternIsNull;

	public FilterColumnPattern(
		FilterPatternType type,
		Pattern dbPattern,
		Pattern tablePattern,
		String columnName,
		Pattern columnPattern
	) {
		super(type, dbPattern, tablePattern);
		this.columnName = columnName;
		this.columnPattern = columnPattern;
		this.columnPatternIsNull = "^null$".equals(columnPattern.toString().toLowerCase());
	}

	@Override
	public void match(String database, String table, FilterResult match) { }

	@Override
	public void matchValue(String database, String table, Map<String, Object> data, FilterResult match) {
		boolean applyFilter = false;
		if ( appliesTo(database, table) && data.containsKey(columnName) ) {
			Object value = data.get(columnName);

			if ( columnPatternIsNull ) {
				// null or "null" (string) or "NULL" (string) is expected
				if (value == null || "null".equals(value) || "NULL".equals(value)) {
					applyFilter = true;
				}
			} else if ( value == null ) {
				// wildcards match the null value
				if ( columnPattern.pattern().length() == 0 )
					applyFilter = true;
			} else {
				if ( columnPattern.matcher(value.toString()).find() )
					applyFilter = true;
			}
		}

		if ( applyFilter )
			match.include = (this.type == FilterPatternType.INCLUDE);
	}

	@Override
	public boolean couldIncludeColumn(String database, String table, Set<String> columns) {
		return type == FilterPatternType.INCLUDE
			&& appliesTo(database, table)
			&& columns.contains(columnName);
	}

	@Override
	public String toString() {
		String filterString = super.toString();
		return filterString + "." + columnName + "=" + patternToString(columnPattern);
	}
}
