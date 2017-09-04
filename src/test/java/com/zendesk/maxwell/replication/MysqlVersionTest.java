package com.zendesk.maxwell.replication;

import com.zendesk.maxwell.TestWithNameLogging;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MysqlVersionTest extends TestWithNameLogging {
	@Test
	public void testVersionComparison() {
		MysqlVersion version = new MysqlVersion(6, 5);
		assertTrue(version.atLeast(6, 5));
		assertTrue(version.atLeast(6, 4));
		assertTrue(version.atLeast(5, 9));

		assertFalse(version.atLeast(7, 2));
		assertFalse(version.atLeast(6, 6));
	}
}
