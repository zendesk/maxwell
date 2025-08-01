package com.zendesk.maxwell.schema.ddl;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.List;

import org.junit.Test;

public class MariaDBSyntaxTest {

	private List<SchemaChange> parse(String sql) {
		return SchemaChange.parse("default_db", sql);
	}

	@Test
	public void testMariaDBIfExistsBasic() {
		String[] testCases = {
			"ALTER TABLE test_table IF EXISTS ADD COLUMN new_col INT",
			"ALTER TABLE test_table IF EXISTS DROP COLUMN old_col",
			"ALTER TABLE test_table IF EXISTS MODIFY COLUMN id BIGINT"
		};

		for (String sql : testCases) {
			try {
				List<SchemaChange> result = parse(sql);
				assertThat("Expected " + sql + " to parse", result, is(not(nullValue())));
				assertThat("Expected " + sql + " to have results", result.size() > 0, is(true));
			} catch (Exception e) {
				System.err.println("Failed to parse: " + sql);
				System.err.println("Error: " + e.getMessage());
				throw e;
			}
		}
	}

	@Test
	public void testMariaDBIndexIfExists() {
		String[] testCases = {
			"ALTER TABLE test_table ADD INDEX IF NOT EXISTS idx_name (name)",
			"ALTER TABLE test_table DROP INDEX IF EXISTS idx_nonexistent"
		};

		for (String sql : testCases) {
			try {
				List<SchemaChange> result = parse(sql);
				assertThat("Expected " + sql + " to parse", result, is(not(nullValue())));
				assertThat("Expected " + sql + " to have results", result.size() > 0, is(true));
			} catch (Exception e) {
				System.err.println("Failed to parse: " + sql);
				System.err.println("Error: " + e.getMessage());
				throw e;
			}
		}
	}

	@Test
	public void testMariaDBConstraintIfExists() {
		String[] testCases = {
			"ALTER TABLE test_table ADD CONSTRAINT IF NOT EXISTS uk_email UNIQUE (email)",
			"ALTER TABLE test_table DROP CONSTRAINT IF EXISTS uk_nonexistent"
		};

		for (String sql : testCases) {
			try {
				List<SchemaChange> result = parse(sql);
				assertThat("Expected " + sql + " to parse", result, is(not(nullValue())));
				assertThat("Expected " + sql + " to have results", result.size() > 0, is(true));
			} catch (Exception e) {
				System.err.println("Failed to parse: " + sql);
				System.err.println("Error: " + e.getMessage());
				throw e;
			}
		}
	}
}
