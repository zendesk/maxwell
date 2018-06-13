package com.zendesk.maxwell.core;

import com.zendesk.maxwell.core.config.MaxwellConfig;
import com.zendesk.maxwell.core.config.MaxwellFilter;
import com.zendesk.maxwell.core.config.MaxwellOutputConfig;
import com.zendesk.maxwell.core.config.EncryptionMode;
import com.zendesk.maxwell.core.row.RowMap;
import com.zendesk.maxwell.core.config.MaxwellConfigFactory;
import com.zendesk.maxwell.core.row.BaseRowMap;
import com.zendesk.maxwell.core.schema.SchemaStoreSchema;
import com.zendesk.maxwell.core.util.test.mysql.MysqlIsolatedServer;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.sql.ResultSet;
import java.util.*;
import java.util.regex.Pattern;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import static org.junit.Assume.assumeTrue;


public class MaxwellIntegrationTest extends MaxwellTestWithIsolatedServer {

	@Autowired
	private MaxwellConfigFactory maxwellConfigFactory;
	@Autowired
	private SchemaStoreSchema schemaStoreSchema;

	@Test
	public void testEncryptedData() throws Exception{
		MaxwellOutputConfig outputConfig = new MaxwellOutputConfig();
		outputConfig.encryptionMode = EncryptionMode.ENCRYPT_DATA;
		outputConfig.secretKey = "aaaaaaaaaaaaaaaa";
		List<RowMap> list;
		String input[] = {"insert into minimal set account_id =1, text_field='hello'"};
		list = getRowsForSQL(input);
		String json = list.get(0).toJSON(outputConfig);

		Map<String,Object> output = maxwellTestJSON.parseJSON(json);
		Map<String, Object> decrypted = maxwellTestJSON.parseEncryptedJSON(output, outputConfig.secretKey);

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

		Map<String,Object> output = maxwellTestJSON.parseJSON(json);
		Map<String, Object> decrypted = maxwellTestJSON.parseEncryptedJSON(output, outputConfig.secretKey);

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
		assertThat(list.get(0).pkToJson(BaseRowMap.KeyFormat.HASH), is(expectedJSON));
	}

	@Test
	public void testCaseSensitivePrimaryKeyStrings() throws Exception {
		List<RowMap> list;
		String before[] = { "create table pksen (Id int, primary key(ID))" };
		String input[] = {"insert into pksen set id =1"};
		String expectedJSON = "{\"database\":\"shard_1\",\"table\":\"pksen\",\"pk.id\":1}";
		list = getRowsForSQL(null, input, before);
		assertThat(list.size(), is(1));
		assertThat(list.get(0).pkToJson(BaseRowMap.KeyFormat.HASH), is(expectedJSON));
	}

	@Test
	public void testPrimaryKeyWithSetType() throws Exception {
		List<RowMap> list;
		String before[] = { "create table pksen (Id set('android','iphone','ipad'), primary key(ID))" };
		String input[] = {"insert into pksen set id ='android'"};
		String expectedJSON = "{\"database\":\"shard_1\",\"table\":\"pksen\",\"pk.id\":[\"android\"]}";
		list = getRowsForSQL(null, input, before);
		assertThat(list.size(), is(1));
		assertThat(list.get(0).pkToJson(BaseRowMap.KeyFormat.HASH), is(expectedJSON));
	}

	@Test
	public void testAlternativePKString() throws Exception {
		List<RowMap> list;
		String input[] = {"insert into minimal set account_id =1, text_field='hello'"};
		String expectedJSON = "[\"shard_1\",\"minimal\",[{\"id\":1},{\"text_field\":\"hello\"}]]";
		list = getRowsForSQL(input);
		assertThat(list.size(), is(1));
		assertThat(list.get(0).pkToJson(BaseRowMap.KeyFormat.ARRAY), is(expectedJSON));
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
		if (MysqlIsolatedServer.inGtidMode()) {
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

	private static String createDBs[] = {
		"CREATE database if not exists foo",
		"CREATE table if not exists foo.bars ( id int(11) auto_increment not null, something text, primary key (id) )",
	};

	private static String insertSQL[] = {
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

	@Test
	public void testExcludeColumns() throws Exception {
		List<RowMap> list;
		MaxwellFilter filter = new MaxwellFilter();

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

	private static String blacklistSQLDDL[] = {
		"CREATE DATABASE nodatabase",
		"CREATE TABLE nodatabase.noseeum (i int)",
		"CREATE TABLE nodatabase.oicu (i int)"
	};

	private static String blacklistSQLDML[] = {
		"insert into nodatabase.noseeum set i = 1",
		"insert into nodatabase.oicu set i = 1"
	};

	@Test
	public void testDDLTableBlacklist() throws Exception {
		server.execute("drop database if exists nodatabase");
		MaxwellFilter filter = new MaxwellFilter();
		filter.blacklistTable("noseeum");

		String[] allSQL = ArrayUtils.addAll(blacklistSQLDDL, blacklistSQLDML);

		List<RowMap> rows = getRowsForSQL(filter, allSQL);
		assertThat(rows.size(), is(1));
	}

	@Test
	public void testDDLDatabaseBlacklist() throws Exception {
		server.execute("drop database if exists nodatabase");

		MaxwellFilter filter = new MaxwellFilter();
		filter.blacklistDatabases("nodatabase");

		String[] allSQL = ArrayUtils.addAll(blacklistSQLDDL, blacklistSQLDML);

		List<RowMap> rows = getRowsForSQL(filter, allSQL);
		assertThat(rows.size(), is(0));
	}

	private String testAlterSQL[] = {
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

	private String testTransactions[] = {
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
	public void testRunMinimalBinlog() throws Exception {
		requireMinimumVersion(MysqlIsolatedServer.VERSION_5_6);

		try {
			server.getConnection().createStatement().execute("set global binlog_row_image='minimal'");
			server.resetConnection(); // only new connections pick up the binlog setting

			runJSON("json/test_minimal");
		} finally {
			server.getConnection().createStatement().execute("set global binlog_row_image='full'");
			server.resetConnection();
		}
	}

	@Test
	public void testRunMainJSONTest() throws Exception {
		runJSON("json/test_1j");
	}

	@Test
	public void testCreateLikeJSON() throws Exception {
		runJSON("json/test_create_like");
	}

	@Test
	public void testCreateSelectJSON() throws Exception {
		if (MysqlIsolatedServer.inGtidMode()) {
			// "CREATE TABLE ... SELECT is forbidden when @@GLOBAL.ENFORCE_GTID_CONSISTENCY = 1"
			return;
		}
		runJSON("json/test_create_select");
	}

	@Test
	public void testEnumJSON() throws Exception {
		runJSON("json/test_enum");
	}

	@Test
	public void testLatin1JSON() throws Exception {
		runJSON("json/test_latin1");
	}

	@Test
	public void testSetJSON() throws Exception {
		runJSON("json/test_set");
	}

	@Test
	public void testZeroCreatedAtJSON() throws Exception {
		assumeTrue(server.supportsZeroDates());
		runJSON("json/test_zero_created_at");
	}

	@Test
	public void testLowerCasingSensitivity() throws Exception {
		MysqlIsolatedServer lowerCaseServer = new MysqlIsolatedServer();


		lowerCaseServer.boot("--lower-case-table-names=1");
		MaxwellContext context = maxwellConfigTestSupport.buildContextWithBufferedProducerFor(lowerCaseServer.getPort(), null, null);
		schemaStoreSchema.ensureMaxwellSchema(lowerCaseServer.getConnection(), context.getConfig().getDatabaseName());

		String[] sql = {
			"CREATE TABLE `test`.`TOOTOOTWEE` ( id int )",
			"insert into `test`.`tootootwee` set id = 5"
		};

		List<RowMap> rows = maxwellTestSupport.getRowsWithReplicator(lowerCaseServer, null, sql, null);
		assertThat(rows.size(), is(1));
		assertThat(rows.get(0).getTable(), is("tootootwee"));
	}

	@Test
	public void testBlob() throws Exception {
		runJSON("json/test_blob");
	}

	@Test
	public void testBit() throws Exception {
		runJSON("json/test_bit");
	}

	@Test
	public void testBignum() throws Exception {
		runJSON("json/test_bignum");
	}

	@Test
	public void testTime() throws Exception {
		requireMinimumVersion(MysqlIsolatedServer.VERSION_5_6);
		runJSON("json/test_time");
	}

	@Test
	public void testInvalid() throws Exception {
		requireMinimumVersion(MysqlIsolatedServer.VERSION_5_6);
		runJSON("json/test_invalid_time");
	}
	@Test
	public void testUCS2() throws Exception {
		runJSON("json/test_ucs2");
	}

	@Test
	public void testCharsets() throws Exception {
		runJSON("json/test_charsets");
	}

	@Test
	public void testGIS() throws Exception {
		runJSON("json/test_gis");
	}

	@Test
	public void testColumnCase() throws Exception {
		runJSON("json/test_column_case");
	}

	@Test
	public void testJson() throws Exception {
		requireMinimumVersion(MysqlIsolatedServer.VERSION_5_7);
		runJSON("json/test_json");
	}

	private static String[] createDBSql = {
			"CREATE database if not exists `foo`",
			"CREATE TABLE if not exists `foo`.`ordered_output` ( id int, account_id int, user_id int )"
	};
	private static String[] insertDBSql = {
			"insert into `foo`.`ordered_output` set id = 1, account_id = 2, user_id = 3"
	};

	@Test
	public void testOrderedOutput() throws Exception {
		MaxwellFilter filter = new MaxwellFilter();
		List<RowMap> rows = getRowsForSQL(filter, insertDBSql, createDBSql);
		String ordered_data = "\"data\":\\{\"id\":1,\"account_id\":2,\"user_id\":3\\}";
		assertTrue(Pattern.compile(ordered_data).matcher(rows.get(0).toJSON()).find());
	}

	@Test
	public void testJdbcConnectionOptions() throws Exception {
		Properties options = new Properties();
		options.put("jdbc_options", "netTimeoutForStreamingResults=123& profileSQL=true ");
		options.put("host", "no-soup-spoons");

		MaxwellConfig config = maxwellConfigFactory.createFor(options);
		config.validate();
		assertThat(config.getMaxwellMysql().getConnectionURI(), containsString("jdbc:mysql://no-soup-spoons:3306/maxwell?"));
		assertThat(config.getReplicationMysql().getConnectionURI(), containsString("jdbc:mysql://no-soup-spoons:3306?"));

		Set<String> maxwellMysqlParams = new HashSet<>(Arrays.asList(config.getMaxwellMysql().getConnectionURI().split("\\?")[1].split("&")));
		assertThat(maxwellMysqlParams, hasItem("zeroDateTimeBehavior=convertToNull"));
		assertThat(maxwellMysqlParams, hasItem("connectTimeout=5000"));
		assertThat(maxwellMysqlParams, hasItem("netTimeoutForStreamingResults=123"));
		assertThat(maxwellMysqlParams, hasItem("profileSQL=true"));

		Set<String> replicationMysqlParams = new HashSet<>(Arrays.asList(config.getReplicationMysql().getConnectionURI().split("\\?")[1].split("&")));
		assertThat(replicationMysqlParams, hasItem("zeroDateTimeBehavior=convertToNull"));
		assertThat(replicationMysqlParams, hasItem("connectTimeout=5000"));
		assertThat(replicationMysqlParams, hasItem("netTimeoutForStreamingResults=123"));
		assertThat(replicationMysqlParams, hasItem("profileSQL=true"));
	}

	@Test
	public void testSchemaServerDifferentThanReplicationServer() throws Exception {
		Properties options = new Properties();
		options.put("replication_host", "replhost");
		options.put("replication_port", "1001");
		options.put("replication_user", "repluser");
		options.put("replication_password", "replpass");
		options.put("schema_host", "schemahost");
		options.put("schema_port", "2002");
		options.put("schema_user", "schemauser");
		options.put("schema_password", "schemapass");

		MaxwellConfig config = maxwellConfigFactory.createFor(options);
		assertEquals(config.getReplicationMysql().host, "replhost");
		assertThat(config.getReplicationMysql().port, is(1001));
		assertEquals(config.getReplicationMysql().user, "repluser");
		assertEquals(config.getReplicationMysql().password, "replpass");
		assertEquals(config.getSchemaMysql().host, "schemahost");
		assertThat(config.getSchemaMysql().port, is(2002));
		assertEquals(config.getSchemaMysql().user, "schemauser");
		assertEquals(config.getSchemaMysql().password, "schemapass");
	}

	@Test
	public void testSchemaServerNotSet() throws Exception {
		Properties options = new Properties();
		options.put("replication_host", "replhost");
		options.put("replication_port", "1001");
		options.put("replication_user", "repluser");
		options.put("replication_password", "replpass");

		MaxwellConfig config = maxwellConfigFactory.createFor(options);
		assertEquals(config.getReplicationMysql().host, "replhost");
		assertThat(config.getReplicationMysql().port, is(1001));
		assertEquals(config.getReplicationMysql().user, "repluser");
		assertEquals(config.getReplicationMysql().password, "replpass");
		assertNull(config.getSchemaMysql().host);
		assertNull(config.getSchemaMysql().user);
		assertNull(config.getSchemaMysql().password);
	}

	@Test
	public void testRowQueryLogEventsIsOn() throws Exception {
		requireMinimumVersion(MysqlIsolatedServer.VERSION_5_6);
		MaxwellOutputConfig outputConfig = new MaxwellOutputConfig();
		outputConfig.includesRowQuery = true;

		runJSON("json/test_row_query_log_is_on", outputConfig);
	}
}
