package com.zendesk.maxwell.filtering;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import static org.junit.Assert.*;

public class FilterV2Test {
	private List<FilterPattern> filters;

	private List<FilterPattern> runParserTest(String input) throws Exception {
		FilterParser parser = new FilterParser(input);
		return parser.parse();
	}

	@Test
	public void TestParser() throws Exception {
		filters = runParserTest("include:/foo.*/.bar");

		assertEquals(1, filters.size());
		assertEquals(FilterPatternType.INCLUDE, filters.get(0).getType());
		assertEquals(Pattern.compile("foo.*").toString(), filters.get(0).getDatabasePattern().toString());
		assertEquals(Pattern.compile("^bar$").toString(), filters.get(0).getTablePattern().toString());
	}

	@Test
	public void TestBlacklists() throws Exception {
		filters = runParserTest("blacklist:foo.*");
		assertEquals(1, filters.size());
		assertEquals(FilterPatternType.BLACKLIST, filters.get(0).getType());
		assertEquals(Pattern.compile("^foo$").toString(), filters.get(0).getDatabasePattern().toString());
	}
}
