package com.zendesk.maxwell.filtering;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import static org.junit.Assert.*;

public class FilterTest {
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

	@Test
	public void TestQuoting() throws Exception {
		String tests[] = {
			"include:`foo`.*",
			"include:'foo'.*",
			"include:\"foo\".*"
		};
		for ( String test : tests ) {
			filters = runParserTest(test);
			assertEquals(1, filters.size());
			assertEquals(Pattern.compile("^foo$").toString(), filters.get(0).getDatabasePattern().toString());
		}
	}

	@Test
	public void TestOddNames() throws Exception {
		runParserTest("include: 1foo.bar");
		runParserTest("include: _foo._bar");

	}

	@Test
	public void TestAdvancedRegexp() throws Exception {
		String pattern = "\\w+ \\/[a-z]*1";
		filters = runParserTest("include: /" + pattern + "/.*");
		assertEquals(1, filters.size());
		assertEquals(Pattern.compile(pattern).toString(), filters.get(0).getDatabasePattern().toString());
	}

	@Test
	public void TestExcludeAll() throws Exception {
		Filter f = new Filter("exclude: *.*, include: foo.bar", "");
		assertTrue(f.includes("foo", "bar"));
		assertFalse(f.includes("anything", "else"));
	}

	@Test
	public void TestBlacklist() throws Exception {
		Filter f = new Filter("blacklist: seria.*", "");
		assertTrue(f.includes("foo", "bar"));
		assertFalse(f.includes("seria", "var"));
		assertTrue(f.isDatabaseBlacklisted("seria"));
		assertTrue(f.isTableBlacklisted("seria", "anything"));
	}
}
