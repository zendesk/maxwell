package com.zendesk.maxwell;

import static org.junit.Assert.*;

import java.sql.SQLException;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.*;

import com.google.code.or.common.util.MySQLConstants;
import com.zendesk.maxwell.schema.Database;
import com.zendesk.maxwell.schema.Schema;
import com.zendesk.maxwell.schema.SchemaCapturer;
import com.zendesk.maxwell.schema.Table;
import com.zendesk.maxwell.schema.columndef.*;

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

		assertEquals("shard_1:shard_2:test", dbs);
	}

	@Test
	public void testOneDatabase() throws SQLException {
		SchemaCapturer sc = new SchemaCapturer(server.getConnection(), "shard_1");
		Schema s = sc.capture();

		String dbs = StringUtils.join(s.getDatabaseNames().iterator(), ":");
		assertEquals("shard_1", dbs);
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

		ColumnDef columns[];

		columns = sharded.getColumnList().toArray(new ColumnDef[0]);

		assertThat(columns[0], notNullValue());
		assertThat(columns[0], instanceOf(BigIntColumnDef.class));
		assertThat(columns[0].getName(), is("id"));
		assertEquals(0, columns[0].getPos());

		assertTrue(columns[0].matchesMysqlType(MySQLConstants.TYPE_LONGLONG));
		assertFalse(columns[0].matchesMysqlType(MySQLConstants.TYPE_DECIMAL));

		assertThat(columns[1], allOf(notNullValue(), instanceOf(IntColumnDef.class)));
		assertThat(columns[1].getName(), is("account_id"));
	}
}
