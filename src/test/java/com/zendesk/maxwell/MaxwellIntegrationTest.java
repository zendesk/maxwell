package com.zendesk.maxwell;

import com.google.common.collect.Lists;
import com.zendesk.maxwell.filtering.Filter;
import com.zendesk.maxwell.producer.EncryptionMode;
import com.zendesk.maxwell.producer.MaxwellOutputConfig;
import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.schema.SchemaStoreSchema;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.Before;
import org.junit.Test;

import java.sql.ResultSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;


public class MaxwellIntegrationTest extends MaxwellTestWithIsolatedServer {
	@Test
	public void testEncryptedData() throws Exception{
		MaxwellOutputConfig outputConfig = new MaxwellOutputConfig();
		outputConfig.encryptionMode = EncryptionMode.ENCRYPT_DATA;
		outputConfig.secretKey = "aaaaaaaaaaaaaaaa";
		List<RowMap> list;
		String input[] = {"insert into minimal set account_id =1, text_field='hello'"};
		list = getRowsForSQL(input);
		String json = list.get(0).toJSON(outputConfig);

		Map<String,Object> output = MaxwellTestJSON.parseJSON(json);
		Map<String, Object> decrypted = MaxwellTestJSON.parseEncryptedJSON(output, outputConfig.secretKey);

		assertTrue(output.get("database").equals("shard_1"));
		assertTrue(output.get("table").equals("minimal"));
		assertTrue(Pattern.matches("\\d+", output.get("xid").toString()));
		assertTrue(output.get("type").equals("insert"));
		assertTrue(Pattern.matches("\\d+",output.get("ts").toString()));
		assertTrue(output.get("commit").equals(true));
		assertTrue(((Map) decrypted.get("data")).get("account_id").equals(1));
		assertTrue(((Map) decrypted.get("data")).get("text_field").equals("hello"));
	}

	@Test
	public void testEncryptedAll() throws Exception{
		MaxwellOutputConfig outputConfig = new MaxwellOutputConfig();
		outputConfig.encryptionMode = EncryptionMode.ENCRYPT_ALL;
		outputConfig.secretKey = "aaaaaaaaaaaaaaaa";
		List<RowMap> list;
		String input[] = {"insert into minimal set account_id =1, text_field='hello'"};
		list = getRowsForSQL(input);
		String json = list.get(0).toJSON(outputConfig);

		Map<String,Object> output = MaxwellTestJSON.parseJSON(json);
		Map<String, Object> decrypted = MaxwellTestJSON.parseEncryptedJSON(output, outputConfig.secretKey);

		assertArrayEquals(output.keySet().toArray(), new String[]{ "encrypted" });

		assertTrue(decrypted.get("database").equals("shard_1"));
		assertTrue(decrypted.get("table").equals("minimal"));
		assertTrue(Pattern.matches("\\d+", decrypted.get("xid").toString()));
		assertTrue(decrypted.get("type").equals("insert"));
		assertTrue(Pattern.matches("\\d+",decrypted.get("ts").toString()));
		assertTrue(decrypted.get("commit").equals(true));
		assertTrue(((Map) decrypted.get("data")).get("account_id").equals(1));
		assertTrue(((Map) decrypted.get("data")).get("text_field").equals("hello"));
	}
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
		assertThat(list.get(0).pkToJson(RowMap.KeyFormat.HASH), is(expectedJSON));
	}

	@Test
	public void testCaseSensitivePrimaryKeyStrings() throws Exception {
		List<RowMap> list;
		String before[] = { "create table pksen (Id int, primary key(ID))" };
		String input[] = {"insert into pksen set id =1"};
		String expectedJSON = "{\"database\":\"shard_1\",\"table\":\"pksen\",\"pk.id\":1}";
		list = getRowsForSQL(null, input, before);
		assertThat(list.size(), is(1));
		assertThat(list.get(0).pkToJson(RowMap.KeyFormat.HASH), is(expectedJSON));
	}

	@Test
	public void testPrimaryKeyWithSetType() throws Exception {
		List<RowMap> list;
		String before[] = { "create table pksen (Id set('android','iphone','ipad'), primary key(ID))" };
		String input[] = {"insert into pksen set id ='android'"};
		String expectedJSON = "{\"database\":\"shard_1\",\"table\":\"pksen\",\"pk.id\":[\"android\"]}";
		list = getRowsForSQL(null, input, before);
		assertThat(list.size(), is(1));
		assertThat(list.get(0).pkToJson(RowMap.KeyFormat.HASH), is(expectedJSON));
	}

	@Test
	public void testAlternativePKString() throws Exception {
		List<RowMap> list;
		String input[] = {"insert into minimal set account_id =1, text_field='hello'"};
		String expectedJSON = "[\"shard_1\",\"minimal\",[{\"id\":1},{\"text_field\":\"hello\"}]]";
		list = getRowsForSQL(input);
		assertThat(list.size(), is(1));
		assertThat(list.get(0).pkToJson(RowMap.KeyFormat.ARRAY), is(expectedJSON));
	}

	@Test
	public void testOutputConfig() throws Exception {
		List<RowMap> list;
		String input[] = {"insert into minimal set account_id =1, text_field='hello'"};
		MaxwellOutputConfig outputConfig = new MaxwellOutputConfig();

		outputConfig.includesCommitInfo = true;
		outputConfig.includesBinlogPosition = true;
		outputConfig.includesGtidPosition = true;

		list = getRowsForSQL(input);
		String json = list.get(0).toJSON(outputConfig);

		// Binlog
		if (MaxwellTestSupport.inGtidMode()) {
			assertTrue(Pattern.matches(".*\"gtid\":\".*:.*\".*", json));
		} else {
			assertTrue(Pattern.matches(".*\"position\":\"master.0+1.\\d+\".*", json));
		}
		// Commit
		assertTrue(Pattern.matches(".*\"commit\":true.*", json));
		// Xid
		assertTrue(Pattern.matches(".*\"xid\":\\d+.*", json));

		// by default the server_id and thread_id should not be included in the output
		assertFalse(Pattern.matches(".*\"server_id\":\\d+.*", json));
		assertFalse(Pattern.matches(".*\"thread_id\":\\d+.*", json));
	}

	@Test
	public void testServerId() throws Exception {
		ResultSet resultSet = server.getConnection().createStatement().executeQuery("SELECT @@server_id");
		resultSet.next();
		final long actualServerId = resultSet.getLong(1);

		List<RowMap> list;
		String input[] = {"insert into minimal set account_id =1, text_field='hello'"};
		MaxwellOutputConfig outputConfig = new MaxwellOutputConfig();

		outputConfig.includesServerId = true;

		list = getRowsForSQL(input);
		String json = list.get(0).toJSON(outputConfig);

		assertTrue(Pattern.matches(".*\"server_id\":"+actualServerId+",.*", json));
	}


	@Test
	public void testThreadId() throws Exception {
		assumeFalse(MysqlIsolatedServer.getVersion().isMariaDB);
		ResultSet resultSet = server.getConnection().createStatement().executeQuery("SELECT CONNECTION_ID()");
		resultSet.next();
		final long actualThreadId = resultSet.getLong(1);

		List<RowMap> list;
		String input[] = {"insert into minimal set account_id =1, text_field='hello'"};
		MaxwellOutputConfig outputConfig = new MaxwellOutputConfig();

		outputConfig.includesThreadId = true;

		list = getRowsForSQL(input);
		String json = list.get(0).toJSON(outputConfig);

		assertTrue(Pattern.matches(".*\"thread_id\":"+actualThreadId+",.*", json));
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

		Filter filter = new Filter();

		list = getRowsForSQL(filter, insertSQL, createDBs);
		assertThat(list.size(), is(2));

		r = list.get(0);
		assertThat(r.getTable(), is("bars"));

		filter.addRule("exclude: *.*, include: shard_1.minimal");
		list = getRowsForSQL(filter, insertSQL);
		assertThat(list.size(), is(1));
		assertThat(list.get(0).getTable(), is("minimal"));
	}

	@Test
	public void testExcludeDB() throws Exception {
		List<RowMap> list;

		Filter filter = new Filter();
		filter.addRule("exclude: shard_1.*");
		list = getRowsForSQL(filter, insertSQL, createDBs);
		assertThat(list.size(), is(1));

		assertThat(list.get(0).getTable(), is("bars"));
	}

	@Test
	public void testIncludeTable() throws Exception {
		List<RowMap> list;

		Filter filter = new Filter();
		filter.addRule("exclude: *.*, include: *.minimal");

		list = getRowsForSQL(filter, insertSQL, createDBs);

		assertThat(list.size(), is(1));

		assertThat(list.get(0).getTable(), is("minimal"));
	}

	@Test
	public void testExcludeTable() throws Exception {
		List<RowMap> list;

		Filter filter = new Filter();
		filter.addRule("exclude: *.minimal");

		list = getRowsForSQL(filter, insertSQL, createDBs);

		assertThat(list.size(), is(1));

		assertThat(list.get(0).getTable(), is("bars"));
	}

	@Test
	public void testExcludeColumns() throws Exception {
		List<RowMap> list;
		Filter filter = new Filter();

		list = getRowsForSQL(filter, insertSQL, createDBs);
		String json = list.get(1).toJSON();
		assertTrue(Pattern.compile("\"id\":1").matcher(json).find());
		assertTrue(Pattern.compile("\"account_id\":2").matcher(json).find());

		MaxwellOutputConfig outputConfig = new MaxwellOutputConfig();
		outputConfig.excludeColumns.add(Pattern.compile("id"));

		list = getRowsForSQL(filter, insertSQL, createDBs);
		json = list.get(1).toJSON(outputConfig);

		assertFalse(Pattern.compile("\"id\":\\d+").matcher(json).find());
		assertTrue(Pattern.compile("\"account_id\":2").matcher(json).find());
	}

	static String insertColumnSQL[] = {
		"INSERT into foo.bars set something = 'accept'",
		"INSERT into foo.bars set something = 'reject'"
	};

	@Test
	public void testExcludeColumnValues() throws Exception {
		List<RowMap> list;

		Filter filter = new Filter();
		filter.addRule("exclude: foo.bars.something=reject");

		list = getRowsForSQL(filter, insertColumnSQL, createDBs);

		assertThat(list.size(), is(1));
	}

	@Test
	public void testExcludeTableIncludeColumns() throws Exception {
		List<RowMap> list;

		Filter filter = new Filter();
		filter.addRule("exclude: foo.bars, include: foo.bars.something=accept");

		list = getRowsForSQL(filter, insertColumnSQL, createDBs);

		assertThat(list.size(), is(1));
	}

	@Test
	public void testExcludeIncludeColumns() throws Exception {
		List<RowMap> list;

		Filter filter = new Filter();
		filter.addRule("exclude: foo.bars.something=*, include: foo.bars.something=accept");

		list = getRowsForSQL(filter, insertColumnSQL, createDBs);

		assertThat(list.size(), is(1));
	}

	@Test
	public void testExcludeMissingColumns() throws Exception {
		List<RowMap> list;

		Filter filter = new Filter();
		filter.addRule("exclude: foo.bars.notacolumn=*");

		list = getRowsForSQL(filter, insertColumnSQL, createDBs);

		assertThat(list.size(), is(2));
	}

	@Test
	public void testWildCardMatchesNull() throws Exception {
		List<RowMap> list;

		String nullInsert[] = {
			"INSERT into foo.bars set something = NULL",
			"INSERT into foo.bars set something = 'accept'"
		};

		Filter filter = new Filter();
		filter.addRule("exclude: foo.bars.something=*");

		list = getRowsForSQL(filter, nullInsert, createDBs);

		assertThat(list.size(), is(0));
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
		Filter filter = new Filter();
		filter.addRule("blacklist: *.noseeum");

		String[] allSQL = (String[])ArrayUtils.addAll(blacklistSQLDDL, blacklistSQLDML);

		List<RowMap> rows = getRowsForSQL(filter, allSQL);
		assertThat(rows.size(), is(1));
	}

	@Test
	public void testDDLDatabaseBlacklist() throws Exception {
		server.execute("drop database if exists nodatabase");

		Filter filter = new Filter();
		filter.addRule("blacklist: nodatabase.*");

		String[] allSQL = (String[])ArrayUtils.addAll(blacklistSQLDDL, blacklistSQLDML);

		List<RowMap> rows = getRowsForSQL(filter, allSQL);
		assertThat(rows.size(), is(0));
	}

	String testAlterSQL[] = {
			"insert into minimal set account_id = 1, text_field='hello'",
			"ALTER table minimal drop primary key, drop column text_field, add primary key(id)",
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

	@Test
	public void testSystemBlacklist() throws Exception  {
		String sql[] = {
			"create table mysql.ha_health_check ( id int )",
			"create table mysql.rds_heartbeat2 ( id int )",
			"insert into mysql.ha_health_check set id = 1",
			"insert into mysql.rds_heartbeat2 set id = 1"
		};

		List<RowMap> list = getRowsForSQL(sql);
		assertThat(list.size(), is(0));
	}

	String testTransactions[] = {
			"create table if not exists minimal ( account_id int, text_field text )",
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

		list = getRowsForSQLTransactional(testTransactions);

		assertEquals(4, list.size());
		for ( RowMap r : list ) {
			assertNotNull(r.getXid());
		}

		assertEquals(list.get(0).getXid(), list.get(1).getXid());
		assertFalse(list.get(0).isTXCommit());
		assertTrue(list.get(1).isTXCommit());

		assertFalse(list.get(2).isTXCommit());
		assertTrue(list.get(3).isTXCommit());
	}

	@Test
	public void testHeartbeatsWithBlacklist() throws Exception {
		Filter filter = new Filter("blacklist: maxwell.*");
		getRowsForSQL(filter, testTransactions);

		ResultSet rs = server.query("select * from maxwell.positions");
		rs.next();

		Long heartbeatRead = rs.getLong("last_heartbeat_read");
		assertTrue(heartbeatRead > 0);
	}

	@Test
	public void testRunMinimalBinlog() throws Exception {
		requireMinimumVersion(server.VERSION_5_6);

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
		if (MaxwellTestSupport.inGtidMode()) {
			// "CREATE TABLE ... SELECT is forbidden when @@GLOBAL.ENFORCE_GTID_CONSISTENCY = 1"
			return;
		}
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
		assumeTrue(server.supportsZeroDates());
		runJSON("/json/test_zero_created_at");
	}

	@Test
	public void testLowerCasingSensitivity() throws Exception {
		org.junit.Assume.assumeTrue(MysqlIsolatedServer.getVersion().lessThan(8,0));

		MysqlIsolatedServer lowerCaseServer = new MysqlIsolatedServer();


		lowerCaseServer.boot("--lower-case-table-names=1");
		MaxwellContext context = MaxwellTestSupport.buildContext(lowerCaseServer.getPort(), null, null);
		SchemaStoreSchema.ensureMaxwellSchema(lowerCaseServer.getConnection(), context.getConfig().databaseName);

		String[] sql = {
			"CREATE TABLE `test`.`TOOTOOTWEE` ( id int )",
			"insert into `test`.`tootootwee` set id = 5"
		};

		List<RowMap> rows = MaxwellTestSupport.getRowsWithReplicator(lowerCaseServer, sql, null, null);
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
		requireMinimumVersion(server.VERSION_5_6);
		runJSON("/json/test_time");
	}

	@Test
	public void testInvalid() throws Exception {
		requireMinimumVersion(server.VERSION_5_6);
		runJSON("/json/test_invalid_time");
	}
	@Test
	public void testUCS2() throws Exception {
		runJSON("/json/test_ucs2");
	}

	@Test
	public void testCharsets() throws Exception {
		runJSON("/json/test_charsets");
	}

	@Test
	public void testGIS() throws Exception {
		runJSON("/json/test_gis");
	}

	@Test
	public void testColumnCase() throws Exception {
		runJSON("/json/test_column_case");
	}

	@Test
	public void testJson() throws Exception {
		requireMinimumVersion(server.VERSION_5_7);
		runJSON("/json/test_json");
	}

	@Test
	public void testJavascriptFilters() throws Exception {
		requireMinimumVersion(server.VERSION_5_6);

		String dir = MaxwellTestSupport.getSQLDir();

		if ( !MysqlIsolatedServer.getVersion().isMariaDB ) {
			server.execute("SET binlog_rows_query_log_events=true");
		}
		List<RowMap> rows = runJSON("/json/test_javascript_filters", (c) -> {
			c.javascriptFile = dir + "/json/filter.javascript";
			c.outputConfig.includesRowQuery = true;
		});

		boolean foundPartitionString = false;
		for ( RowMap row : rows ) {
			if ( row.getPartitionString() != null )
				foundPartitionString = true;
		}
		assertTrue(foundPartitionString);
	}

	@Test
	public void testJavascriptFiltersInjectRichValues() throws Exception {
		String dir = MaxwellTestSupport.getSQLDir();
		runJSON("/json/test_javascript_filters_rich_values", (c) -> {
			c.javascriptFile = dir + "/json/filter_rich.javascript";
		});
	}

	static String[] createDBSql = {
			"CREATE database if not exists `foo`",
			"CREATE TABLE if not exists `foo`.`ordered_output` ( id int, account_id int, user_id int )"
	};
	static String[] insertDBSql = {
			"insert into `foo`.`ordered_output` set id = 1, account_id = 2, user_id = 3"
	};

	@Test
	public void testOrderedOutput() throws Exception {
		Filter filter = new Filter();
		List<RowMap> rows = getRowsForSQL(filter, insertDBSql, createDBSql);
		String ordered_data = "\"data\":\\{\"id\":1,\"account_id\":2,\"user_id\":3\\}";
		assertTrue(Pattern.compile(ordered_data).matcher(rows.get(0).toJSON()).find());
	}

	@Test
	public void testJdbcConnectionOptions() throws Exception {
		String[] opts = {"--jdbc_options= netTimeoutForStreamingResults=123& profileSQL=true  ", "--host=no-soup-spoons"};
		MaxwellConfig config = new MaxwellConfig(opts);
		config.validate();
		assertThat(config.maxwellMysql.getConnectionURI(), containsString("jdbc:mysql://no-soup-spoons:3306/maxwell?"));
		assertThat(config.replicationMysql.getConnectionURI(), containsString("jdbc:mysql://no-soup-spoons:3306?"));

		Set<String> maxwellMysqlParams = new HashSet<>();
		maxwellMysqlParams.addAll(Lists.newArrayList(config.maxwellMysql.getConnectionURI()
				.split("\\?")[1].split("&")));

		assertThat(maxwellMysqlParams, hasItem("zeroDateTimeBehavior=convertToNull"));
		assertThat(maxwellMysqlParams, hasItem("connectTimeout=5000"));
		assertThat(maxwellMysqlParams, hasItem("netTimeoutForStreamingResults=123"));
		assertThat(maxwellMysqlParams, hasItem("profileSQL=true"));

		Set<String> replicationMysqlParams = new HashSet<>();
		replicationMysqlParams.addAll(Lists.newArrayList(config.replicationMysql.getConnectionURI()
				.split("\\?")[1].split("&")));

		assertThat(replicationMysqlParams, hasItem("zeroDateTimeBehavior=convertToNull"));
		assertThat(replicationMysqlParams, hasItem("connectTimeout=5000"));
		assertThat(replicationMysqlParams, hasItem("netTimeoutForStreamingResults=123"));
		assertThat(replicationMysqlParams, hasItem("profileSQL=true"));
	}

	@Test
	public void testSchemaServerDifferentThanReplicationServer() throws Exception {
		String[] opts = {
			"--replication_host=replhost",
			"--replication_port=1001",
			"--replication_user=repluser",
			"--replication_password=replpass",
			"--schema_host=schemahost",
			"--schema_port=2002",
			"--schema_user=schemauser",
			"--schema_password=schemapass"
		};
		MaxwellConfig config = new MaxwellConfig(opts);
		assertEquals(config.replicationMysql.host, "replhost");
		assertThat(config.replicationMysql.port, is(1001));
		assertEquals(config.replicationMysql.user, "repluser");
		assertEquals(config.replicationMysql.password, "replpass");
		assertEquals(config.schemaMysql.host, "schemahost");
		assertThat(config.schemaMysql.port, is(2002));
		assertEquals(config.schemaMysql.user, "schemauser");
		assertEquals(config.schemaMysql.password, "schemapass");
	}

	@Test
	public void testSchemaServerNotSet() throws Exception {
		String[] opts = {
			"--replication_host=replhost",
			"--replication_port=1001",
			"--replication_user=repluser",
			"--replication_password=replpass",
		};
		MaxwellConfig config = new MaxwellConfig(opts);
		assertEquals(config.replicationMysql.host, "replhost");
		assertThat(config.replicationMysql.port, is(1001));
		assertEquals(config.replicationMysql.user, "repluser");
		assertEquals(config.replicationMysql.password, "replpass");
		assertNull(config.schemaMysql.host);
		assertNull(config.schemaMysql.user);
		assertNull(config.schemaMysql.password);
	}

	@Test
	public void testRowQueryLogEventsIsOn() throws Exception {
		requireMinimumVersion(server.VERSION_5_6);
		final MaxwellOutputConfig outputConfig = new MaxwellOutputConfig();
		outputConfig.includesRowQuery = true;

		if ( !MysqlIsolatedServer.getVersion().isMariaDB ) {
			server.execute("SET binlog_rows_query_log_events=true");
		}

		runJSON("/json/test_row_query_log_is_on", (c) -> c.outputConfig = outputConfig);
	}

}
