package com.zendesk.maxwell.filtering;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.junit.Assert.*;

public class FilterTest {
	private List<FilterPattern> filters;

	private List<FilterPattern> runParserTest(String input) throws Exception {
		return new FilterParser(input).parse();
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
	public void TestColumnFilterParse() throws Exception {
		String input = "include: 1foo.bar.column=foo";
		filters = runParserTest(input);
		assertEquals(1, filters.size());
		assertEquals(input, filters.get(0).toString());
	}

	@Test
	public void TestExcludeAll() throws Exception {
		Filter f = new Filter("exclude: *.*, include: foo.bar");
		assertTrue(f.includes("foo", "bar"));
		assertFalse(f.includes("anything", "else"));
	}

	@Test
	public void TestBlacklist() throws Exception {
		Filter f = new Filter("blacklist: seria.*");
		assertTrue(f.includes("foo", "bar"));
		assertFalse(f.includes("seria", "var"));
		assertTrue(f.isDatabaseBlacklisted("seria"));
		assertTrue(f.isTableBlacklisted("seria", "anything"));
	}

	@Test
	public void TestColumnFiltersAreIgnoredByIncludes() throws Exception {
		Filter f = new Filter("exclude: *.*, include: foo.bar.col=val");
		assertFalse(f.includes("foo", "bar"));
	}

	@Test
	public void TestColumnFilters() throws Exception {
		Map<String, Object> map = new HashMap<>();
		map.put("col", "val");
		Filter f = new Filter("exclude: *.*, include: foo.bar.col=val");
		assertFalse(f.includes("foo", "bar"));
		assertTrue(f.includes("foo", "bar", map));
	}

	@Test
	public void TestColumnFiltersFromOtherTables() throws Exception {
		Map<String, Object> map = new HashMap<>();
		map.put("col", "val");
		Filter f = new Filter("exclude: *.*, include: no.go.col=val");
		assertFalse(f.includes("foo", "bar"));
		assertFalse(f.includes("foo", "bar", map));
	}

	@Test
	public void TestNullValuesInData() throws Exception {
		Map<String, Object> map = new HashMap<>();
		map.put("col", null);
		Filter f = new Filter("exclude: *.*, include: foo.bar.col=null");
		assertFalse(f.includes("foo", "bar"));
		assertTrue(f.includes("foo", "bar", map));

		map.put("col", "null");
		assertTrue(f.includes("foo", "bar", map));

		map.remove("col");
		assertFalse(f.includes("foo", "bar", map));

	}

	@Test
	public void TestSetValidFilter() throws Exception {
		Filter f = new Filter("exclude: *.*, include: foo.bar.col=null");
		assertEquals(f.toString(), "exclude: *.*, include: foo.bar.col=null");
		f.set("blacklist: seria.*");
		assertEquals(f.toString(), "blacklist: seria.*");
	}

	@Test
	public void TestSetInvalidFilter() throws Exception {
		Filter f = new Filter("exclude: *.*, include: foo.bar.col=null");
		assertEquals(f.toString(), "exclude: *.*, include: foo.bar.col=null");
		try {
			f.set("bl: seria.*");
		} catch (InvalidFilterException e) {
			// do nothing
		}
		assertEquals(f.toString(), "exclude: *.*, include: foo.bar.col=null");
	}

	@Test
	public void TestToString() throws Exception {
		Filter f = new Filter("exclude: *.*, include: foo.bar.col=null");
		assertEquals(f.toString(), "exclude: *.*, include: foo.bar.col=null");
	}

	@Test
	public void TestEmptyToString() throws Exception {
		Filter f = new Filter("");
		assertEquals(f.toString(), "");
	}
}

