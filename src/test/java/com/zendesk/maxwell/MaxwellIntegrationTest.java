package com.zendesk.maxwell;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;
import org.apache.commons.lang.ArrayUtils;
import java.util.List;

import com.zendesk.maxwell.schema.SchemaStore;
import org.junit.Test;

public class MaxwellIntegrationTest extends MaxwellTestWithIsolatedServer {
	@Test
	public void testGetEvent() throws Exception {
		List<RowMap> list;
		String input[] = {"insert into minimal set account_id = 1, text_field='hello'"};
		list = getRowsForSQL(input);
		assertThat(list.size(), is(1));
	}

	@Test
	public void testPrimaryKeyStrings() throws Exception {
		List<RowMap> list;
		String input[] = {"insert into minimal set account_id =1, text_field='hello'"};
		String expectedJSON = "{\"database\":\"shard_1\",\"table\":\"minimal\",\"pk.id\":1,\"pk.text_field\":\"hello\"}";
		list = getRowsForSQL(input);
		assertThat(list.size(), is(1));
		assertThat(list.get(0).pkToJson(), is(expectedJSON));
	}

	@Test
	public void testRowFilter() throws Exception {
		List<RowMap> list;
		String input[] = {"insert into minimal set account_id = 1000, text_field='hello'",
						  "insert into minimal set account_id = 2000, text_field='goodbye'"};

		list = getRowsForSQL(input);
		assertThat(list.size(), is(2));

		MaxwellFilter filter = new MaxwellFilter();

		@SuppressWarnings("UnnecessaryBoxing")
		Integer filterValue = new Integer(2000); // make sure we're using a different instance of the filter value

		filter.addRowConstraint("account_id", filterValue);

		list = getRowsForSQL(filter, input);
		assertThat(list.size(), is(1));

		RowMap jsonMap = list.get(0);

		assertThat((Long) jsonMap.getData("account_id"), is(2000L));
		assertThat((String) jsonMap.getData("text_field"), is("goodbye"));
	}

	@Test
	public void testRowFilterOnNonExistentFields() throws Exception {
		List<RowMap> list;
		String input[] = {"insert into minimal set account_id = 1000, text_field='hello'",
						  "insert into minimal set account_id = 2000, text_field='goodbye'"};

		MaxwellFilter filter = new MaxwellFilter();
		filter.addRowConstraint("piggypiggy", 2000);

		list = getRowsForSQL(filter, input);
		assertThat(list.size(), is(0));
	}

	static String createDBs[] = {
		"CREATE database if not exists foo",
		"CREATE table if not exists foo.bars ( id int(11) auto_increment not null, something text, primary key (id) )",
	};

	static String insertSQL[] = {
		"INSERT into foo.bars set something = 'hi'",
		"INSERT into shard_1.minimal set account_id = 2, text_field='sigh'"
	};

	@Test
	public void testIncludeDB() throws Exception {
		List<RowMap> list;
		RowMap r;

		MaxwellFilter filter = new MaxwellFilter();

		list = getRowsForSQL(filter, insertSQL, createDBs);
		assertThat(list.size(), is(2));

		r = list.get(0);
		assertThat(r.getTable(), is("bars"));

		filter.includeDatabase("shard_1");
		list = getRowsForSQL(filter, insertSQL);
		assertThat(list.size(), is(1));
		assertThat(list.get(0).getTable(), is("minimal"));
	}

	@Test
	public void testExcludeDB() throws Exception {
		List<RowMap> list;

		MaxwellFilter filter = new MaxwellFilter();
		filter.excludeDatabase("shard_1");
		list = getRowsForSQL(filter, insertSQL, createDBs);
		assertThat(list.size(), is(1));

		assertThat(list.get(0).getTable(), is("bars"));
	}

	@Test
	public void testIncludeTable() throws Exception {
		List<RowMap> list;

		MaxwellFilter filter = new MaxwellFilter();
		filter.includeTable("minimal");

		list = getRowsForSQL(filter, insertSQL, createDBs);

		assertThat(list.size(), is(1));

		assertThat(list.get(0).getTable(), is("minimal"));
	}

	@Test
	public void testExcludeTable() throws Exception {
		List<RowMap> list;

		MaxwellFilter filter = new MaxwellFilter();
		filter.excludeTable("minimal");

		list = getRowsForSQL(filter, insertSQL, createDBs);

		assertThat(list.size(), is(1));

		assertThat(list.get(0).getTable(), is("bars"));
	}

	static String blacklistSQLDDL[] = {
		"CREATE DATABASE nodatabase",
		"CREATE TABLE nodatabase.noseeum (i int)",
		"CREATE TABLE nodatabase.oicu (i int)"
	};

	static String blacklistSQLDML[] = {
		"insert into nodatabase.noseeum set i = 1",
		"insert into nodatabase.oicu set i = 1"
	};

	@Test
	public void testDDLTableBlacklist() throws Exception {
		server.execute("drop database if exists nodatabase");
		MaxwellFilter filter = new MaxwellFilter();
		filter.blacklistTable("noseeum");

		String[] allSQL = (String[])ArrayUtils.addAll(blacklistSQLDDL, blacklistSQLDML);

		List<RowMap> rows = getRowsForSQL(filter, allSQL);
		assertThat(rows.size(), is(1));
	}

	@Test
	public void testDDLDatabaseBlacklist() throws Exception {
		server.execute("drop database if exists nodatabase");

		MaxwellFilter filter = new MaxwellFilter();
		filter.blacklistDatabases("nodatabase");

		String[] allSQL = (String[])ArrayUtils.addAll(blacklistSQLDDL, blacklistSQLDML);

		List<RowMap> rows = getRowsForSQL(filter, allSQL);
		assertThat(rows.size(), is(0));
	}

	String testAlterSQL[] = {
			"insert into minimal set account_id = 1, text_field='hello'",
			"ALTER table minimal drop column text_field",
			"insert into minimal set account_id = 2",
			"ALTER table minimal add column new_text_field varchar(255)",
			"insert into minimal set account_id = 2, new_text_field='hihihi'",
	};

	@Test
	public void testAlterTable() throws Exception {
		List<RowMap> list;

		list = getRowsForSQL(testAlterSQL);

		assertThat(list.get(0).getTable(), is("minimal"));
	}

	@Test
	public void testMyISAMCommit() throws Exception {
		String sql[] = {
				"CREATE TABLE myisam_test ( id int ) engine=myisam",
				"insert into myisam_test (id) values (1), (2), (3)"

		};

		List<RowMap> list = getRowsForSQL(sql);
		assertThat(list.size(), is(3));
		assertThat(list.get(2).isTXCommit(), is(true));
	}

	String testTransactions[] = {
			"BEGIN",
			"insert into minimal set account_id = 1, text_field = 's'",
			"insert into minimal set account_id = 2, text_field = 's'",
			"COMMIT",
			"BEGIN",
			"insert into minimal (account_id, text_field) values (3, 's'), (4, 's')",
			"COMMIT"
	};

	@Test
	public void testTransactionID() throws Exception {
		List<RowMap> list;

		try {
			server.getConnection().setAutoCommit(false);
			list = getRowsForSQL(testTransactions);

			assertEquals(4, list.size());
			for ( RowMap r : list ) {
				assertNotNull(r.getXid());
			}

			assertEquals(list.get(0).getXid(), list.get(1).getXid());
			assertFalse(list.get(0).isTXCommit());
			assertTrue(list.get(1).isTXCommit());

			assertFalse(list.get(2).isTXCommit());
			assertTrue(list.get(3).isTXCommit());
		} finally {
			server.getConnection().setAutoCommit(true);
		}
	}

	@Test
	public void testRunMinimalBinlog() throws Exception {
		if ( server.getVersion().equals("5.5") )
			return;

		try {
			server.getConnection().createStatement().execute("set global binlog_row_image='minimal'");
			server.resetConnection(); // only new connections pick up the binlog setting

			runJSON("/json/test_minimal");
		} finally {
			server.getConnection().createStatement().execute("set global binlog_row_image='full'");
			server.resetConnection();
		}
	}

	@Test
	public void testRunMainJSONTest() throws Exception {
		runJSON("/json/test_1j");
	}

	@Test
	public void testCreateLikeJSON() throws Exception {
		runJSON("/json/test_create_like");
	}

	@Test
	public void testCreateSelectJSON() throws Exception {
		runJSON("/json/test_create_select");
	}

	@Test
	public void testEnumJSON() throws Exception {
		runJSON("/json/test_enum");
	}

	@Test
	public void testLatin1JSON() throws Exception {
		runJSON("/json/test_latin1");
	}

	@Test
	public void testSetJSON() throws Exception {
		runJSON("/json/test_set");
	}

	@Test
	public void testZeroCreatedAtJSON() throws Exception {
		if ( server.getVersion().equals("5.5") ) // 5.6 not yet supported for this test
			runJSON("/json/test_zero_created_at");
	}

	@Test
	public void testLowerCasingSensitivity() throws Exception {
		MysqlIsolatedServer lowerCaseServer = new MysqlIsolatedServer();


		lowerCaseServer.boot("--lower-case-table-names=1");
		MaxwellContext context = MaxwellTestSupport.buildContext(lowerCaseServer.getPort(), null);
		SchemaStore.ensureMaxwellSchema(lowerCaseServer.getConnection(), context.getConfig().databaseName);

		String[] sql = {
			"CREATE TABLE `test`.`TOOTOOTWEE` ( id int )",
			"insert into `test`.`tootootwee` set id = 5"
		};

		List<RowMap> rows = MaxwellTestSupport.getRowsForSQL(lowerCaseServer, null, sql, null);
		assertThat(rows.size(), is(1));
		assertThat(rows.get(0).getTable(), is("tootootwee"));
	}

	@Test
	public void testBlob() throws Exception {
		runJSON("/json/test_blob");
	}

	@Test
	public void testBit() throws Exception {
		runJSON("/json/test_bit");
	}

	@Test
	public void testBignum() throws Exception {
		runJSON("/json/test_bignum");
	}

	@Test
	public void testTime() throws Exception {
		if ( server.getVersion().equals("5.6") )
			runJSON("/json/test_time");
	}

	@Test
	public void testUCS2() throws Exception {
		runJSON("/json/test_ucs2");
	}

	@Test
	public void testGIS() throws Exception {
		runJSON("/json/test_gis");
	}

}
