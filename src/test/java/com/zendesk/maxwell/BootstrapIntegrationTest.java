package com.zendesk.maxwell;

import com.zendesk.maxwell.row.RowEncrypt;
import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.producer.MaxwellOutputConfig;
import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import org.apache.commons.codec.binary.Base64;
import org.junit.Test;

import java.nio.charset.Charset;
import java.sql.Timestamp;
import java.util.*;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;

public class BootstrapIntegrationTest extends MaxwellTestWithIsolatedServer {
	@Test
	public void testSingleRowBootstrap() throws Exception {
		runJSON("json/bootstrap-single-row");
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
	public void testBootstrapIsWhitelisted() throws Exception {
		MaxwellFilter filter = new MaxwellFilter();
		filter.includeDatabase("shard_1");
		runJSON("json/bootstrap-whitelist", filter);
	}

	@Test
	public void testBootstrapNullValues() throws Exception {
		runJSON("json/bootstrap-null-values");
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
		String epoch = String.valueOf(new Timestamp(0)); // timezone dependent
		testColumnType("datetime", "'1000-01-01 00:00:00'","1000-01-01 00:00:00", null);

		testColumnType("tinytext", "'hello'", "hello");
		testColumnType("text", "'hello'", "hello");
		testColumnType("mediumtext","'hello'", "hello");
		testColumnType("longtext","'hello'", "hello");
		testColumnType("varchar(10)","'hello'", "hello");
		testColumnType("char", "'h'", "h");
		testColumnType("date", "'2015-11-07'","2015-11-07");
		testColumnType("datetime", "'2015-11-07 01:02:03'","2015-11-07 01:02:03");

		if ( !server.getVersion().equals("5.7") ) {
			testColumnType("date", "'0000-00-00'",null);
			testColumnType("datetime", "'0000-00-00 00:00:00'", null);
			testColumnType("timestamp", "'0000-00-00 00:00:00'","" + epoch.substring(0, epoch.length() - 2) + "", null);
		}

		testColumnType("datetime", "'1000-01-01 00:00:00'","1000-01-01 00:00:00");

		testColumnType("timestamp", "'2015-11-07 01:02:03'","2015-11-07 01:02:03");

		testColumnType("enum('a', 'b')","'a'", "a");
		testColumnType("bit(8)","b'01010101'", 85);
		testColumnType("bit(8)","b'1'", 1);
	}

	@Test
	public void testSubsecondTypes() throws Exception {
		if ( server.getVersion().equals("5.6") ) {
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
			testColumnType("time(3)", "'01:02:03.123456'","01:02:03.123");
			testColumnType("time(6)", "'01:02:03.123456'","01:02:03.123456");
			testColumnType("time(3)", "'01:02:03.123'","01:02:03.123");
		}
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
		testEncryptedColumnType(sqlType, sqlValue, expectedJsonValue, expectedJsonValue);
		testEncryptedAllColumnType(sqlType, sqlValue, expectedJsonValue, expectedJsonValue);
	}

	private void testColumnType(String sqlType, String sqlValue, Object expectedNormalJsonValue, Object expectedBootstrappedJsonValue) throws Exception {
		String input[] = {
			"DROP TABLE IF EXISTS shard_1.column_test",
			String.format("CREATE TABLE IF NOT EXISTS shard_1.column_test (id int unsigned auto_increment NOT NULL primary key, col %s)", sqlType),
			String.format("INSERT INTO shard_1.column_test SET col = %s", sqlValue),
			"INSERT INTO maxwell.bootstrap set database_name = 'shard_1', table_name = 'column_test'"
		};

		List<RowMap> rows = getRowsForSQL(input);
		boolean foundNormalRow = false;

		for ( RowMap r : rows ) {
			String json = r.toJSON();

			Map<String, Object> data, output = MaxwellTestJSON.parseJSON(r.toJSON());
			if ( output.get("table").equals("column_test") && output.get("type").equals("insert") ) {
				data = (Map<String, Object>) output.get("data");
				if ( !foundNormalRow ) {
					foundNormalRow = true;
					assertThat(data.get("col"), is(expectedNormalJsonValue));
				} else {
					assertThat(data.get("col"), is(expectedBootstrappedJsonValue));
				}
			}
		}
	}

	private void testEncryptedColumnType(String sqlType, String sqlValue, Object expectedNormalJsonValue, Object expectedBootstrappedJsonValue) throws Exception {
		MaxwellOutputConfig outputConfig = new MaxwellOutputConfig();
		outputConfig.encryptData = true;
		outputConfig.encryption_key = "aaaaaaaaaaaaaaaa";
		outputConfig.secret_key = "RandomInitVector";

		String input[] = {
				"DROP TABLE IF EXISTS shard_1.column_test",
				String.format("CREATE TABLE IF NOT EXISTS shard_1.column_test (id int unsigned auto_increment NOT NULL primary key, col %s)", sqlType),
				String.format("INSERT INTO shard_1.column_test SET col = %s", sqlValue),
				"INSERT INTO maxwell.bootstrap set database_name = 'shard_1', table_name = 'column_test'"
		};

		List<RowMap> rows = getRowsForSQL(input);
		boolean foundNormalRow = false;

		for ( RowMap r : rows ) {
			String json = r.toJSON(outputConfig);

			Map<String, Object> data, output = MaxwellTestJSON.parseJSON(r.toJSON(outputConfig));
			if ( output.get("table").equals("column_test") && output.get("type").equals("insert") ) {
				IvParameterSpec ivSpec = new IvParameterSpec(outputConfig.secret_key.getBytes("UTF-8"));
				SecretKeySpec skeySpec = new SecretKeySpec(outputConfig.encryption_key.getBytes("UTF-8"), "AES");
				Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5PADDING");
				cipher.init(Cipher.DECRYPT_MODE, skeySpec, ivSpec);

				output.put("data",MaxwellTestJSON.parseJSON(new String(cipher.doFinal(Base64.decodeBase64(output.get("data").toString().getBytes())), Charset.forName("UTF-8"))));

				data = (Map<String, Object>) output.get("data");
				if ( !foundNormalRow ) {
					foundNormalRow = true;
					assertThat(data.get("col"), is(expectedNormalJsonValue));
				} else {
					assertThat(data.get("col"), is(expectedBootstrappedJsonValue));
				}
			}
		}
	}

	private void testEncryptedAllColumnType(String sqlType, String sqlValue, Object expectedNormalJsonValue, Object expectedBootstrappedJsonValue) throws Exception {
		MaxwellOutputConfig outputConfig = new MaxwellOutputConfig();
		outputConfig.encryptAll = true;
		outputConfig.encryption_key = "aaaaaaaaaaaaaaaa";
		outputConfig.secret_key = "RandomInitVector";

		String input[] = {
				"DROP TABLE IF EXISTS shard_1.column_test",
				String.format("CREATE TABLE IF NOT EXISTS shard_1.column_test (id int unsigned auto_increment NOT NULL primary key, col %s)", sqlType),
				String.format("INSERT INTO shard_1.column_test SET col = %s", sqlValue),
				"INSERT INTO maxwell.bootstrap set database_name = 'shard_1', table_name = 'column_test'"
		};

		List<RowMap> rows = getRowsForSQL(input);
		boolean foundNormalRow = false;

		for ( RowMap r : rows ) {
			String json = r.toJSON(outputConfig);

			Map<String,Object> output = MaxwellTestJSON.parseJSON(RowEncrypt.decrypt(json, outputConfig.encryption_key, outputConfig.secret_key));
			if ( output.get("table").equals("column_test") && output.get("type").equals("insert") ) {
				output = (Map<String, Object>) output.get("data");
				if ( !foundNormalRow ) {
					foundNormalRow = true;
					assertThat(output.get("col"), is(expectedNormalJsonValue));
				} else {
					assertThat(output.get("col"), is(expectedBootstrappedJsonValue));
				}
			}
		}
	}
}
