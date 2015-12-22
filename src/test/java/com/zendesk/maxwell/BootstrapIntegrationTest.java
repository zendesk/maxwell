package com.zendesk.maxwell;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;

public class BootstrapIntegrationTest extends AbstractMaxwellTest {
	@Test
		public void testSingleRowBootstrap() throws Exception {
		List<RowMap> list;
		String input[] = {
			"insert into minimal set account_id = 1, text_field='hello'",
			"insert into maxwell.bootstrap set database_name = 'shard_1', table_name = 'minimal'"
		};
		String expectedJSON[] = {
			"{\"database\":\"shard_1\",\"table\":\"minimal\",\"type\":\"insert\",\"ts\":0,\"xid\":0,\"data\":{\"id\":0,\"account_id\":1,\"text_field\":\"hello\"}}",
			"{\"database\":\"maxwell\",\"table\":\"bootstrap\",\"type\":\"insert\",\"ts\":0,\"xid\":0,\"data\":{\"id\":0,\"database_name\":\"shard_1\",\"table_name\":\"minimal\",\"is_complete\":0}}",
			"{\"database\":\"shard_1\",\"table\":\"minimal\",\"type\":\"bootstrap-start\",\"ts\":0,\"data\":{}}",
			"{\"database\":\"shard_1\",\"table\":\"minimal\",\"type\":\"insert\",\"ts\":0,\"data\":{\"id\":0,\"account_id\":1,\"text_field\":\"hello\"}}",
			"{\"database\":\"maxwell\",\"table\":\"bootstrap\",\"type\":\"update\",\"ts\":0,\"xid\":0,\"data\":{\"id\":0,\"database_name\":\"shard_1\",\"completed_at\":\"\",\"table_name\":\"minimal\",\"started_at\":\"\",\"is_complete\":1},\"old\":{\"completed_at\":null,\"is_complete\":0}}",
			"{\"database\":\"shard_1\",\"table\":\"minimal\",\"type\":\"bootstrap-complete\",\"ts\":0,\"data\":{}}"
		};
		list = getRowsForSQL(null, input, null, false);
		assertThat(list.size(), is(expectedJSON.length));
		for (int i = 0; i < expectedJSON.length; ++i) {
			assertThat(i + " : " + filterJsonForTest(list.get(i).toJSON()), is(i + " : " + expectedJSON[i]));
		}
	}

	@Test
	public void testMultipleRowBootstrap() throws Exception {
		List<RowMap> list;
		String input[] = {
			"insert into minimal set account_id = 1, text_field='hello'",
			"insert into minimal set account_id = 2, text_field='bonjour'",
			"insert into minimal set account_id = 3, text_field='goeiedag'",
			"insert into maxwell.bootstrap set database_name = 'shard_1', table_name = 'minimal'"
		};
		String expectedJSON[] = {
			"{\"database\":\"shard_1\",\"table\":\"minimal\",\"type\":\"insert\",\"ts\":0,\"xid\":0,\"data\":{\"id\":0,\"account_id\":1,\"text_field\":\"hello\"}}",
			"{\"database\":\"shard_1\",\"table\":\"minimal\",\"type\":\"insert\",\"ts\":0,\"xid\":0,\"data\":{\"id\":0,\"account_id\":2,\"text_field\":\"bonjour\"}}",
			"{\"database\":\"shard_1\",\"table\":\"minimal\",\"type\":\"insert\",\"ts\":0,\"xid\":0,\"data\":{\"id\":0,\"account_id\":3,\"text_field\":\"goeiedag\"}}",
			"{\"database\":\"maxwell\",\"table\":\"bootstrap\",\"type\":\"insert\",\"ts\":0,\"xid\":0,\"data\":{\"id\":0,\"database_name\":\"shard_1\",\"table_name\":\"minimal\",\"is_complete\":0}}",
			"{\"database\":\"shard_1\",\"table\":\"minimal\",\"type\":\"bootstrap-start\",\"ts\":0,\"data\":{}}",
			"{\"database\":\"shard_1\",\"table\":\"minimal\",\"type\":\"insert\",\"ts\":0,\"data\":{\"id\":0,\"account_id\":1,\"text_field\":\"hello\"}}",
			"{\"database\":\"shard_1\",\"table\":\"minimal\",\"type\":\"insert\",\"ts\":0,\"data\":{\"id\":0,\"account_id\":2,\"text_field\":\"bonjour\"}}",
			"{\"database\":\"shard_1\",\"table\":\"minimal\",\"type\":\"insert\",\"ts\":0,\"data\":{\"id\":0,\"account_id\":3,\"text_field\":\"goeiedag\"}}",
			"{\"database\":\"maxwell\",\"table\":\"bootstrap\",\"type\":\"update\",\"ts\":0,\"xid\":0,\"data\":{\"id\":0,\"database_name\":\"shard_1\",\"completed_at\":\"\",\"table_name\":\"minimal\",\"started_at\":\"\",\"is_complete\":1},\"old\":{\"completed_at\":null,\"is_complete\":0}}",
			"{\"database\":\"shard_1\",\"table\":\"minimal\",\"type\":\"bootstrap-complete\",\"ts\":0,\"data\":{}}"
		};
		list = getRowsForSQL(null, input, null, false);
		assertThat(list.size(), is(expectedJSON.length));
		for (int i = 0; i < expectedJSON.length; ++i) {
			assertThat(i + " : " + filterJsonForTest(list.get(i).toJSON()), is(i + " : " + expectedJSON[i]));
		}
	}
	
	@Test
	public void testMultipleTablesBootstrap() throws Exception {
		String input[] = {
				"DROP TABLE IF EXISTS shard_1.table1",
				"DROP TABLE IF EXISTS shard_1.table2",
				"DROP TABLE IF EXISTS shard_1.table3",
				"CREATE TABLE IF NOT EXISTS shard_1.table1 (id int unsigned auto_increment NOT NULL primary key, txt varchar(255))",
				"CREATE TABLE IF NOT EXISTS shard_1.table2 (id int unsigned auto_increment NOT NULL primary key, txt varchar(255))",
				"CREATE TABLE IF NOT EXISTS shard_1.table3 (id int unsigned auto_increment NOT NULL primary key, txt varchar(255))",
				"INSERT INTO shard_1.table1 (txt) values (\"txt1\"), (\"txt2\"), (\"txt3\")",
				"INSERT INTO shard_1.table2 (txt) values (\"txt4\"), (\"txt5\"), (\"txt6\")",
				"INSERT INTO shard_1.table3 (txt) values (\"txt7\"), (\"txt8\"), (\"txt9\")",
				"INSERT INTO maxwell.bootstrap set database_name = 'shard_1', table_name = 'table1'",
				"INSERT INTO maxwell.bootstrap set database_name = 'shard_1', table_name = 'table2'",
				"INSERT INTO maxwell.bootstrap set database_name = 'shard_1', table_name = 'table3'"
		};
		String expectedJSONArrays[][] = {
			{
				"{\"database\":\"shard_1\",\"table\":\"table1\",\"type\":\"insert\",\"ts\":0,\"xid\":0,\"data\":{\"id\":0,\"txt\":\"txt1\"}}",
				"{\"database\":\"shard_1\",\"table\":\"table1\",\"type\":\"insert\",\"ts\":0,\"xid\":0,\"data\":{\"id\":0,\"txt\":\"txt2\"}}",
				"{\"database\":\"shard_1\",\"table\":\"table1\",\"type\":\"insert\",\"ts\":0,\"xid\":0,\"data\":{\"id\":0,\"txt\":\"txt3\"}}",
				"{\"database\":\"shard_1\",\"table\":\"table2\",\"type\":\"insert\",\"ts\":0,\"xid\":0,\"data\":{\"id\":0,\"txt\":\"txt4\"}}",
				"{\"database\":\"shard_1\",\"table\":\"table2\",\"type\":\"insert\",\"ts\":0,\"xid\":0,\"data\":{\"id\":0,\"txt\":\"txt5\"}}",
				"{\"database\":\"shard_1\",\"table\":\"table2\",\"type\":\"insert\",\"ts\":0,\"xid\":0,\"data\":{\"id\":0,\"txt\":\"txt6\"}}",
				"{\"database\":\"shard_1\",\"table\":\"table3\",\"type\":\"insert\",\"ts\":0,\"xid\":0,\"data\":{\"id\":0,\"txt\":\"txt7\"}}",
				"{\"database\":\"shard_1\",\"table\":\"table3\",\"type\":\"insert\",\"ts\":0,\"xid\":0,\"data\":{\"id\":0,\"txt\":\"txt8\"}}",
				"{\"database\":\"shard_1\",\"table\":\"table3\",\"type\":\"insert\",\"ts\":0,\"xid\":0,\"data\":{\"id\":0,\"txt\":\"txt9\"}}"
			},
			{
				"{\"database\":\"maxwell\",\"table\":\"bootstrap\",\"type\":\"insert\",\"ts\":0,\"xid\":0,\"data\":{\"id\":0,\"database_name\":\"shard_1\",\"table_name\":\"table1\",\"is_complete\":0}}",
				"{\"database\":\"maxwell\",\"table\":\"bootstrap\",\"type\":\"update\",\"ts\":0,\"xid\":0,\"data\":{\"id\":0,\"database_name\":\"shard_1\",\"completed_at\":\"\",\"table_name\":\"table1\",\"started_at\":\"\",\"is_complete\":1},\"old\":{\"completed_at\":null,\"is_complete\":0}}",
				"{\"database\":\"maxwell\",\"table\":\"bootstrap\",\"type\":\"insert\",\"ts\":0,\"xid\":0,\"data\":{\"id\":0,\"database_name\":\"shard_1\",\"table_name\":\"table2\",\"is_complete\":0}}",
				"{\"database\":\"maxwell\",\"table\":\"bootstrap\",\"type\":\"update\",\"ts\":0,\"xid\":0,\"data\":{\"id\":0,\"database_name\":\"shard_1\",\"completed_at\":\"\",\"table_name\":\"table2\",\"started_at\":\"\",\"is_complete\":1},\"old\":{\"completed_at\":null,\"is_complete\":0}}",
				"{\"database\":\"maxwell\",\"table\":\"bootstrap\",\"type\":\"insert\",\"ts\":0,\"xid\":0,\"data\":{\"id\":0,\"database_name\":\"shard_1\",\"table_name\":\"table3\",\"is_complete\":0}}",
				"{\"database\":\"maxwell\",\"table\":\"bootstrap\",\"type\":\"update\",\"ts\":0,\"xid\":0,\"data\":{\"id\":0,\"database_name\":\"shard_1\",\"completed_at\":\"\",\"table_name\":\"table3\",\"started_at\":\"\",\"is_complete\":1},\"old\":{\"completed_at\":null,\"is_complete\":0}}",
			},
			{
				"{\"database\":\"shard_1\",\"table\":\"table1\",\"type\":\"bootstrap-start\",\"ts\":0,\"data\":{}}",
				"{\"database\":\"shard_1\",\"table\":\"table1\",\"type\":\"insert\",\"ts\":0,\"data\":{\"id\":0,\"txt\":\"txt1\"}}",
				"{\"database\":\"shard_1\",\"table\":\"table1\",\"type\":\"insert\",\"ts\":0,\"data\":{\"id\":0,\"txt\":\"txt2\"}}",
				"{\"database\":\"shard_1\",\"table\":\"table1\",\"type\":\"insert\",\"ts\":0,\"data\":{\"id\":0,\"txt\":\"txt3\"}}",
				"{\"database\":\"shard_1\",\"table\":\"table1\",\"type\":\"bootstrap-complete\",\"ts\":0,\"data\":{}}",
			},
			{
				"{\"database\":\"shard_1\",\"table\":\"table2\",\"type\":\"bootstrap-start\",\"ts\":0,\"data\":{}}",
				"{\"database\":\"shard_1\",\"table\":\"table2\",\"type\":\"insert\",\"ts\":0,\"data\":{\"id\":0,\"txt\":\"txt4\"}}",
				"{\"database\":\"shard_1\",\"table\":\"table2\",\"type\":\"insert\",\"ts\":0,\"data\":{\"id\":0,\"txt\":\"txt5\"}}",
				"{\"database\":\"shard_1\",\"table\":\"table2\",\"type\":\"insert\",\"ts\":0,\"data\":{\"id\":0,\"txt\":\"txt6\"}}",
				"{\"database\":\"shard_1\",\"table\":\"table2\",\"type\":\"bootstrap-complete\",\"ts\":0,\"data\":{}}",
			},
			{
				"{\"database\":\"shard_1\",\"table\":\"table3\",\"type\":\"bootstrap-start\",\"ts\":0,\"data\":{}}",
				"{\"database\":\"shard_1\",\"table\":\"table3\",\"type\":\"insert\",\"ts\":0,\"data\":{\"id\":0,\"txt\":\"txt7\"}}",
				"{\"database\":\"shard_1\",\"table\":\"table3\",\"type\":\"insert\",\"ts\":0,\"data\":{\"id\":0,\"txt\":\"txt8\"}}",
				"{\"database\":\"shard_1\",\"table\":\"table3\",\"type\":\"insert\",\"ts\":0,\"data\":{\"id\":0,\"txt\":\"txt9\"}}",
				"{\"database\":\"shard_1\",\"table\":\"table3\",\"type\":\"bootstrap-complete\",\"ts\":0,\"data\":{}}"
			}
		};
		List<RowMap> rowMaps = getRowsForSQL(null, input, null, false);
		List<String> rowStrings = new ArrayList<>();
		assertThat(rowMaps.size(), is(30));
		for (int i = 0; i < rowMaps.size(); ++i) {
			rowStrings.add(filterJsonForTest(rowMaps.get(i).toJSON()));
		}
		for (int i = 0; i < expectedJSONArrays.length; ++i) {
			String expectedJSON[] = expectedJSONArrays[i];
			int fromIndex = 0;
			for (int j = 0; j < expectedJSON.length; ++j) {
				fromIndex = findFromIndex(fromIndex, expectedJSON[i], rowStrings);
				assertThat(expectedJSON[i] + " not found!", fromIndex, not(-1));
			}
		}
	}

	private int findFromIndex(int fromIndex, String json, List<String> rows) {
		return rows.subList(fromIndex, rows.size()).indexOf(json);
	}

	@Test
	public void testBool() throws Exception {
		testColumnType("bool", "0", "0");
		testColumnType("bool", "1", "1");
	}

	@Test
	public void testBoolean() throws Exception {
		testColumnType("boolean", "0", "0");
		testColumnType("boolean", "1", "1");
	}

	@Test
	public void testTinyInt() throws Exception {
		testColumnType("tinyint", "-128", "-128");
		testColumnType("tinyint", "127", "127");
		testColumnType("tinyint unsigned", "0", "0");
		testColumnType("tinyint unsigned", "255", "255");
	}

	@Test
	public void testSmallInt() throws Exception {
		testColumnType("smallint", "-32768", "-32768");
		testColumnType("smallint", "32767", "32767");
		testColumnType("smallint unsigned", "0", "0");
		testColumnType("smallint unsigned", "65535", "65535");
	}

	@Test
	public void testMediumInt() throws Exception {
		testColumnType("mediumint", "-8388608", "-8388608");
		testColumnType("mediumint", "8388607", "8388607");
		testColumnType("mediumint unsigned", "0", "0");
		testColumnType("mediumint unsigned", "16777215", "16777215");
	}

	@Test
	public void testInt() throws Exception {
		testColumnType("int", "-2147483648", "-2147483648");
		testColumnType("int", "2147483647", "2147483647");
		testColumnType("int unsigned", "0", "0");
		//  throws MySQLDataException
		//testColumnType("int unsigned", "4294967295", "4294967295");
	}

	@Test
	public void testBigInt() throws Exception {
		testColumnType("bigint", "-9223372036854775808", "-9223372036854775808");
		testColumnType("bigint", "9223372036854775807", "9223372036854775807");
		testColumnType("bigint unsigned", "0", "0");
		//  throws MySQLDataException
		//testColumnType("bigint unsigned", "18446744073709551615", "18446744073709551615");
	}

	@Test
	public void testStringTypes( ) throws Exception {
		testColumnType("tinytext", "'hello'", "\"hello\"");
		testColumnType("text", "'hello'", "\"hello\"");
		testColumnType("mediumtext","'hello'", "\"hello\"");
		testColumnType("longtext","'hello'", "\"hello\"");
		testColumnType("varchar(10)","'hello'", "\"hello\"");
		testColumnType("char", "'h'", "\"h\"");
		testColumnType("date", "'2015-11-07'","\"2015-11-07\"");
		testColumnType("datetime", "'2015-11-07 01:02:03'","\"2015-11-07 01:02:03\"");
		testColumnType("timestamp", "'2015-11-07 01:02:03'","\"2015-11-07 01:02:03\"");
		testColumnType("set('a', 'b')","'a,b'", "[\"a\",\"b\"]");
		testColumnType("enum('a', 'b')","'a'", "\"a\"");
		testColumnType("bit(8)","b'01010101'", "85");
		testColumnType("bit(8)","b'1'", "1");
	}

	@Test
	public void testBinaryTypes( ) throws Exception {
		testColumnType("tinyblob", "x'0A0B0C0D0E0F'", "\"CgsMDQ4P\"");
		testColumnType("blob", "x'0A0B0C0D0E0F'", "\"CgsMDQ4P\"");
		testColumnType("mediumblob","x'0A0B0C0D0E0F'", "\"CgsMDQ4P\"");
		testColumnType("longblob","x'0A0B0C0D0E0F'", "\"CgsMDQ4P\"");
		testColumnType("binary(6)","x'0A0B0C0D0E0F'", "\"CgsMDQ4P\"");
		testColumnType("binary(6)","x'0A0B0C0D0E0F'", "\"CgsMDQ4P\"");
		// Unlike MySQL, the Maxwell replicator does not pad short binary values, but the Maxwell bootstrapper does!
		testColumnType("binary(10)","x'0A0B0C0D0E0F'", "\"CgsMDQ4P\"", "\"CgsMDQ4PAAAAAA==\"");
		testColumnType("varbinary(10)","x'0A0B0C0D0E0F'", "\"CgsMDQ4P\"");
	}

	@Test
	public void testOtherNumericTypes() throws Exception {
		testColumnType("real", "3.14159", "3.14159");
		testColumnType("float","3.14159", "3.14159");
		testColumnType("double","3.14159", "3.14159");
		testColumnType("numeric(20,10)","3.14159", "3.1415900000");
		testColumnType("decimal(20,10)","3.14159", "3.1415900000");
		testColumnType("year","2007", "2007");
	}

	private void testColumnType(String sqlType, String sqlValue, String expectedJsonValue) throws Exception {
		testColumnType(sqlType, sqlValue, expectedJsonValue, expectedJsonValue);
	}

	private void testColumnType(String sqlType, String sqlValue, String expectedNormalJsonValue, String expectedBootstrappedJsonValue) throws Exception {
		String input[] = {
			"DROP TABLE IF EXISTS shard_1.column_test",
			String.format("CREATE TABLE IF NOT EXISTS shard_1.column_test (id int unsigned auto_increment NOT NULL primary key, col %s)", sqlType),
			String.format("INSERT INTO shard_1.column_test SET col = %s", sqlValue),
			"INSERT INTO maxwell.bootstrap set database_name = 'shard_1', table_name = 'column_test'"
		};
		String expectedJSON[] = {
			String.format("{\"database\":\"shard_1\",\"table\":\"column_test\",\"type\":\"insert\",\"ts\":0,\"xid\":0,\"data\":{\"id\":0,\"col\":%s}}", expectedNormalJsonValue),
			"{\"database\":\"maxwell\",\"table\":\"bootstrap\",\"type\":\"insert\",\"ts\":0,\"xid\":0,\"data\":{\"id\":0,\"database_name\":\"shard_1\",\"table_name\":\"column_test\",\"is_complete\":0}}",
			"{\"database\":\"shard_1\",\"table\":\"column_test\",\"type\":\"bootstrap-start\",\"ts\":0,\"data\":{}}",
			String.format("{\"database\":\"shard_1\",\"table\":\"column_test\",\"type\":\"insert\",\"ts\":0,\"data\":{\"id\":0,\"col\":%s}}", expectedBootstrappedJsonValue),
			"{\"database\":\"maxwell\",\"table\":\"bootstrap\",\"type\":\"update\",\"ts\":0,\"xid\":0,\"data\":{\"id\":0,\"database_name\":\"shard_1\",\"completed_at\":\"\",\"table_name\":\"column_test\",\"started_at\":\"\",\"is_complete\":1},\"old\":{\"completed_at\":null,\"is_complete\":0}}",
			"{\"database\":\"shard_1\",\"table\":\"column_test\",\"type\":\"bootstrap-complete\",\"ts\":0,\"data\":{}}"
		};
		List<RowMap> rows = getRowsForSQL(null, input, null, false);
		assertThat(rows.size(), is(6));
		for (int i = 0; i < expectedJSON.length; ++i) {
			assertThat(i + " : " + filterJsonForTest(rows.get(i).toJSON()), is(i + " : " + expectedJSON[i]));
		}
	}
}
