package com.zendesk.maxwell;

import com.zendesk.maxwell.filtering.Filter;
import com.zendesk.maxwell.producer.EncryptionMode;
import com.zendesk.maxwell.producer.MaxwellOutputConfig;
import com.zendesk.maxwell.row.RowMap;
import org.junit.Test;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class BootstrapIntegrationTest extends MaxwellTestWithIsolatedServer {
	@Test
	public void testSingleRowBootstrap() throws Exception {
		runJSON("json/bootstrap-single-row");
	}

	@Test
	public void testBootstrapWithComment() throws Exception {
		runJSON("json/bootstrap-with-comment");
	}

	@Test
	public void testMultipleRowBootstrap() throws Exception {
		runJSON("json/bootstrap-multiple-row");
	}

	@Test
	public void testMultipleRowBootstrapWithWhereclause() throws Exception {
		runJSON("json/bootstrap-multiple-row-with-whereclause");
	}

	@Test
	public void testNoPkTableBootstrap() throws Exception {
		runJSON("json/bootstrap-no-pk");
	}

	@Test
	public void testMultipleTablesBootstrap() throws Exception {
		runJSON("json/bootstrap-multiple-tables");
	}

	@Test
	public void testJSONType() throws Exception {
		requireMinimumVersion(server.VERSION_5_7);
		runJSON("json/bootstrap-json-type");
	}

	@Test
	public void testBootstrapIsWhitelisted() throws Exception {
		final Filter filter = new Filter();
		filter.addRule("exclude: *.*, include: shard_1.*");
		runJSON("json/bootstrap-whitelist", (c) -> c.filter = filter);
	}

	@Test
	public void testBootstrapNullValues() throws Exception {
		runJSON("json/bootstrap-null-values");
	}

	@Test
	public void testBootstrapJSFilters() throws Exception {
		String dir = MaxwellTestSupport.getSQLDir();
		runJSON("json/bootstrap-js-filters", (c) -> c.javascriptFile = dir + "/json/filter.javascript");
	}

	@Test
	public void testBootstrapOnSeparateServer() throws Exception {
		MysqlIsolatedServer otherServer = new MysqlIsolatedServer();
		otherServer.boot("--server_id=1231231");

		String sql[] = {
			"create database test_other",
			"create table test_other.other ( id int )",
			"insert into test_other.other set id = 1"
		};

		otherServer.executeList(sql);

		runJSON("json/bootstrap-second-server", (c) -> {
			c.replicationMysql.port = otherServer.getPort();
			try {
				c.initPosition = MaxwellTestSupport.capture(otherServer.getConnection());
			} catch (SQLException e) {
			}
		});
	}

	@Test
	public void testBool() throws Exception {
		testColumnType("bool", "0", 0);
		testColumnType("bool", "1", 1);
	}

	@Test
	public void testBoolean() throws Exception {
		testColumnType("boolean", "0", 0);
		testColumnType("boolean", "1", 1);
	}

	@Test
	public void testTinyInt() throws Exception {
		testColumnType("tinyint", "-128", -128);
		testColumnType("tinyint", "127", 127);
		testColumnType("tinyint unsigned", "0", 0);
		testColumnType("tinyint unsigned", "255", 255);
	}

	@Test
	public void testTinyInt1() throws Exception {
		testColumnType("tinyint(1)", "9", 9);
	}

	@Test
	public void testSmallInt() throws Exception {
		testColumnType("smallint", "-32768", -32768);
		testColumnType("smallint", "32767", 32767);
		testColumnType("smallint unsigned", "0", 0);
		testColumnType("smallint unsigned", "65535", 65535);
	}

	@Test
	public void testMediumInt() throws Exception {
		testColumnType("mediumint", "-8388608", -8388608);
		testColumnType("mediumint", "8388607", 8388607);
		testColumnType("mediumint unsigned", "0", 0);
		testColumnType("mediumint unsigned", "16777215", 16777215);
	}

	@Test
	public void testInt() throws Exception {
		testColumnType("int", "-2147483648", -2147483648);
		testColumnType("int", "2147483647", 2147483647);
		testColumnType("int unsigned", "0", 0);
		// throws MySQLDataException
		//testColumnType("int unsigned", "4294967295", "4294967295");
	}

	@Test
	public void testBigInt() throws Exception {
		testColumnType("bigint", "-9223372036854775808", -9223372036854775808L);
		testColumnType("bigint", "9223372036854775807", 9223372036854775807L);
		testColumnType("bigint unsigned", "0", 0);
		// throws MySQLDataException
		//testColumnType("bigint unsigned", "18446744073709551615", "18446744073709551615");
	}

	@Test
	public void testStringTypes( ) throws Exception {
		testColumnType("datetime", "'1000-01-01 00:00:00'","1000-01-01 00:00:00", "1000-01-01 00:00:00");

		testColumnType("tinytext", "'hello'", "hello");
		testColumnType("text", "'hello'", "hello");
		testColumnType("mediumtext","'hello'", "hello");
		testColumnType("longtext","'hello'", "hello");
		testColumnType("varchar(10)","'hello'", "hello");
		testColumnType("char", "'h'", "h");
		testColumnType("date", "'2015-11-07'","2015-11-07");
		testColumnType("datetime", "'2015-11-07 01:02:03'","2015-11-07 01:02:03");

		testColumnType("datetime", "'1000-01-01 00:00:00'","1000-01-01 00:00:00");

		testColumnType("timestamp", "'2015-11-07 01:02:03'","2015-11-07 01:02:03");

		testColumnType("enum('a', 'b')","'a'", "a");
		testColumnType("bit(8)","b'01010101'", 85);
		testColumnType("bit(8)","b'1'", 1);
	}

	@Test
	public void testZeroDates() throws Exception {
		if (server.supportsZeroDates()) {
			testColumnType("date", "'0000-00-00'", "0000-00-00", null);
			testColumnType("datetime", "'0000-00-00 00:00:00'", "0000-00-00 00:00:00", null);
			testColumnType("timestamp", "'0000-00-00 00:00:00'", "0000-00-00 00:00:00", null);
		}
	}

	@Test
	public void testSubsecondTypes() throws Exception {
		requireMinimumVersion(server.VERSION_5_6);
		testColumnType("time(3)", "'01:02:03.123456'","01:02:03.123");
		testColumnType("time(6)", "'01:02:03.123456'","01:02:03.123456");
		testColumnType("time(3)", "'01:02:03.123'","01:02:03.123");

		testColumnType("timestamp(6)", "'2015-11-07 01:02:03.333444'","2015-11-07 01:02:03.333444");
		testColumnType("timestamp(6)", "'2015-11-07 01:02:03.123'","2015-11-07 01:02:03.123000");
		testColumnType("timestamp(6)", "'2015-11-07 01:02:03.0'","2015-11-07 01:02:03.000000");

		testColumnType("timestamp(3)", "'2015-11-07 01:02:03.123456'","2015-11-07 01:02:03.123");
		testColumnType("timestamp(3)", "'2015-11-07 01:02:03.123'","2015-11-07 01:02:03.123");
		testColumnType("timestamp(3)", "'2015-11-07 01:02:03.1'","2015-11-07 01:02:03.100");
		testColumnType("timestamp(3)", "'2015-11-07 01:02:03.0'","2015-11-07 01:02:03.000");

		testColumnType("datetime(6)", "'2015-11-07 01:02:03.123456'","2015-11-07 01:02:03.123456");
		testColumnType("datetime(6)", "'2015-11-07 01:02:03.123'","2015-11-07 01:02:03.123000");
		testColumnType("datetime(3)", "'2015-11-07 01:02:03.123456'","2015-11-07 01:02:03.123");
		testColumnType("datetime(3)", "'2015-11-07 01:02:03.123'","2015-11-07 01:02:03.123");
	}

	@Test
	public void testSetType() throws Exception {
		ArrayList<String> setValue = new ArrayList<>(2);
		setValue.add("a");
		setValue.add("b");
		testColumnType("set('a', 'b')","'a,b'", setValue);
	}


	@Test
	public void testBinaryTypes( ) throws Exception {
		testColumnType("tinyblob", "x'0A0B0C0D0E0F'", "CgsMDQ4P");
		testColumnType("blob", "x'0A0B0C0D0E0F'", "CgsMDQ4P");
		testColumnType("mediumblob","x'0A0B0C0D0E0F'", "CgsMDQ4P");
		testColumnType("longblob","x'0A0B0C0D0E0F'", "CgsMDQ4P");
		testColumnType("binary(6)","x'0A0B0C0D0E0F'", "CgsMDQ4P");
		testColumnType("binary(6)","x'0A0B0C0D0E0F'", "CgsMDQ4P");
		// Unlike MySQL, the Maxwell replicator does not pad short binary values, but the Maxwell bootstrapper does!
		testColumnType("binary(10)","x'0A0B0C0D0E0F'", "CgsMDQ4P", "CgsMDQ4PAAAAAA==");
		testColumnType("varbinary(10)","x'0A0B0C0D0E0F'", "CgsMDQ4P");
	}

	@Test
	public void testOtherNumericTypes() throws Exception {
		testColumnType("real", "3.14159", 3.14159);
		testColumnType("float","3.14159", 3.14159);
		testColumnType("double","3.14159", 3.14159);
		testColumnType("numeric(20,10)","3.14159", 3.1415900000);
		testColumnType("decimal(20,10)","3.14159", 3.1415900000);
		testColumnType("year","2007", 2007);
	}

	private void testColumnType(String sqlType, String sqlValue, Object expectedJsonValue) throws Exception {
		testColumnType(sqlType, sqlValue, expectedJsonValue, expectedJsonValue);
	}

	private void testColumnType(String sqlType, String sqlValue, Object expectedNormalJsonValue, Object expectedBootstrappedJsonValue) throws Exception {
		String input[] = {
			"DROP TABLE IF EXISTS shard_1.column_test",
			String.format("CREATE TABLE IF NOT EXISTS shard_1.column_test (col %s)", sqlType),
			String.format("INSERT INTO shard_1.column_test SET col = %s", sqlValue),
			"INSERT INTO maxwell.bootstrap set database_name = 'shard_1', table_name = 'column_test'"
		};

		List<RowMap> rows = getRowsForSQL(input);
		testColumnTypeSerialization(EncryptionMode.ENCRYPT_NONE, rows, expectedNormalJsonValue, expectedBootstrappedJsonValue);
		testColumnTypeSerialization(EncryptionMode.ENCRYPT_DATA, rows, expectedNormalJsonValue, expectedBootstrappedJsonValue);
		testColumnTypeSerialization(EncryptionMode.ENCRYPT_ALL, rows, expectedNormalJsonValue, expectedBootstrappedJsonValue);
	}

	private void testColumnTypeSerialization(EncryptionMode encryptionMode, List<RowMap> rows, Object expectedNormalJsonValue, Object expectedBootstrappedJsonValue) throws Exception {
		boolean foundNormalRow = false;
		MaxwellOutputConfig outputConfig = new MaxwellOutputConfig();
		outputConfig.encryptionMode = encryptionMode;
		outputConfig.secretKey = "aaaaaaaaaaaaaaaa";

		for ( RowMap r : rows ) {
			Map<String, Object> output = MaxwellTestJSON.parseJSON(r.toJSON(outputConfig));
			Map<String, Object> decrypted = MaxwellTestJSON.parseEncryptedJSON(output, outputConfig.secretKey);

			if (encryptionMode == EncryptionMode.ENCRYPT_ALL) {
				output = decrypted;
			}

			if ( output.get("table").equals("column_test") && output.get("type").toString().contains("insert") ) {
				Map<String, Object> dataSource = encryptionMode == EncryptionMode.ENCRYPT_DATA ? decrypted : output;
				Map<String, Object> data = (Map<String, Object>) dataSource.get("data");
				if ( output.get("type").equals("insert") ) {
					assertThat(data.get("col"), is(expectedNormalJsonValue));
				} else {
					assertThat(data.get("col"), is(expectedBootstrappedJsonValue));
				}
			}
		}
	}
}
