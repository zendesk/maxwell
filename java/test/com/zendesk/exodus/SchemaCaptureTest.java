package com.zendesk.exodus;

import static org.junit.Assert.assertEquals;

import java.sql.SQLException;

import org.apache.commons.lang.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.zendesk.exodus.schema.Schema;
import com.zendesk.exodus.schema.SchemaCapturer;

public class SchemaCaptureTest extends AbstractMaxwellTest {
	private SchemaCapturer capturer;

	@Before
	public void setUp() throws Exception {
		this.capturer = new SchemaCapturer(server.getConnection());
	}

	@Override
	@After
	public void tearDown() throws Exception {
		super.tearDown();
	}

	@Test
	public void testDatabases() throws SQLException {
		Schema s = capturer.capture();
		String dbs = StringUtils.join(s.getDatabaseNames().iterator(), ":");

		assertEquals("shard_1:shard_2", dbs);
	}
}
