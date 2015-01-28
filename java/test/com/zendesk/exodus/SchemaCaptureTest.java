package com.zendesk.exodus;

import static org.junit.Assert.assertEquals;

import java.sql.SQLException;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.zendesk.exodus.schema.Database;
import com.zendesk.exodus.schema.Schema;
import com.zendesk.exodus.schema.SchemaCapturer;
import com.zendesk.exodus.schema.Table;
import com.zendesk.exodus.schema.column.*;

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

	@Test
	public void testTables() throws SQLException {
		Schema s = capturer.capture();

		Database shard1DB = s.findDatabase("shard_1");
		assert(shard1DB != null);

		List<String> nameList = shard1DB.getTableNames();

		assertEquals("ints:mediumints:minimal:sharded", StringUtils.join(nameList.iterator(), ":"));
	}

	@Test
	public void testColumns() throws SQLException {
		Schema s = capturer.capture();

		Table sharded = s.findDatabase("shard_1").findTable("sharded");
		assert(sharded != null);

		Column columns[];

		columns = sharded.getColumnList().toArray(new Column[0]);

		assert(columns[0] != null);
		assert(columns[0] instanceof IntColumn);

		assertEquals(0, columns[0].getPos());

	}
}
