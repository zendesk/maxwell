package com.zendesk.maxwell.filtering;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class Filter {
	static final Logger LOGGER = LoggerFactory.getLogger(Filter.class);

	private final List<FilterPattern> patterns;
	private String maxwellDB;

	public Filter() {
		this.patterns = new ArrayList<>();
		this.maxwellDB = "maxwell";
	}
	public Filter(String filterString) throws InvalidFilterException {
		this();
		patterns.addAll(new FilterParser(filterString).parse());
	}

	public Filter(String maxwellDB, String filterString) throws InvalidFilterException {
		this();
		this.maxwellDB = maxwellDB;
		patterns.addAll(new FilterParser(filterString).parse());
	}

	@Override
	public String toString() {
		return patterns.stream()
			.map(FilterPattern::toString)
			.collect(Collectors.joining(", "));
	}

	public void set(String filterString) throws InvalidFilterException {
		List<FilterPattern> parsedFilter = new FilterParser(filterString).parse();
		this.patterns.clear();
		this.patterns.addAll(parsedFilter);
	}

	public boolean isSystemWhitelisted(String database, String table) {
		return isMaxwellDB(database)
			&& ("bootstrap".equals(table) || "heartbeats".equals(table));
	}

	public boolean isMaxwellDB(String database) {
		return maxwellDB.equals(database);
	}

	public void addRule(String filterString) throws InvalidFilterException {
		this.patterns.addAll(new FilterParser(filterString).parse());
	}

	public List<FilterPattern> getRules() {
		return new ArrayList<>(this.patterns);
	}


	public boolean includes(String database, String table) {
		FilterResult match = new FilterResult();

		for ( FilterPattern p : patterns )
			p.match(database, table, match);

		return match.include;
	}

	public boolean includes(String database, String table, Map<String, Object> values) {
		FilterResult match = new FilterResult();

		for ( FilterPattern p : patterns )
			p.matchValue(database, table, values, match);

		return match.include;
	}

	public boolean couldIncludeFromColumnFilters(String database, String table, Set<String> columns) {
		for ( FilterPattern p : patterns ) {
			if ( p.couldIncludeColumn(database, table, columns) )
				return true;
		}
		return false;
	}


	public boolean isTableBlacklisted(String database, String table) {
		if ( isSystemBlacklisted(database, table) )
			return true;

		if ( isMaxwellDB(database) )
			return false;

		FilterResult match = new FilterResult();

		for ( FilterPattern p : patterns ) {
			if ( p.getType() == FilterPatternType.BLACKLIST )
				p.match(database, table, match);
		}

		return !match.include;
	}

	public boolean isDatabaseBlacklisted(String database) {
		if ( isMaxwellDB(database) )
			return false;

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

	public static boolean includes(Filter filter, String database, String table, Map<String, Object> data) {
		if (filter == null) {
			return true;
		} else {
			return filter.includes(database, table, data);
		}
	}

	public static boolean couldIncludeFromColumnFilters(Filter filter, String database, String table, Set<String> columnNames) {
		if (filter == null) {
			return false;
		} else {
			return filter.couldIncludeFromColumnFilters(database, table, columnNames);
		}
	}
}
