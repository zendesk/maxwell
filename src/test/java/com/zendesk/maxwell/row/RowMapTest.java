package com.zendesk.maxwell.row;

import com.zendesk.maxwell.MaxwellTestJSON;
import com.zendesk.maxwell.errors.ProtectedAttributeNameException;
import com.zendesk.maxwell.producer.MaxwellOutputConfig;
import com.zendesk.maxwell.replication.BinlogPosition;
import com.zendesk.maxwell.replication.Position;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class RowMapTest {

	private static final long TIMESTAMP_MILLISECONDS = 1496712943447L;

	private static final Position POSITION = new Position(new BinlogPosition(1L, "binlog-0001"), 0L);


	@Test(expected = ProtectedAttributeNameException.class)
	public void testFailOnProtectedAttributes() throws Exception {
		RowMap rowMap = new RowMap("insert", "MyDatabase", "MyTable", 1234567890L, new ArrayList<String>(), null);
		rowMap.putExtraAttribute("table", "bar");
	}

	@Test
	public void testTimestampConversion() throws Exception {
		long timestampSeconds = 1496712943;

		RowMap rowMap = new RowMap("insert", "MyDatabase", "MyTable", TIMESTAMP_MILLISECONDS, new ArrayList<String>(), POSITION);

		Assert.assertEquals(timestampSeconds, rowMap.getTimestamp().longValue());
		Assert.assertEquals(TIMESTAMP_MILLISECONDS, rowMap.getTimestampMillis().longValue());
		Map<String, Object> output = MaxwellTestJSON.parseJSON(rowMap.toJSON());

		int ts = (int) output.get("ts");
		Assert.assertEquals(ts, timestampSeconds);
	}

	@Test
	public void testGetPrimaryKeyArrayValues() {
		List<String> pKeys = new ArrayList<>();

		pKeys.add("id");

		pKeys.add("name");

		RowMap rowMap = new RowMap("insert", "MyDatabase", "MyTable", TIMESTAMP_MILLISECONDS, pKeys, POSITION);

		rowMap.putData("id", 9001);
		rowMap.putData("name", "example");
		rowMap.putData("column", "value");

		List<Object> pkValues = rowMap.getPrimaryKeyValues();
		Assert.assertEquals(9001, pkValues.get(0));
		Assert.assertEquals("example", pkValues.get(1));
	}

	@Test
	public void testGetPrimaryKeyArrayColumns() {
		List<String> pKeys = new ArrayList<>();

		pKeys.add("id");

		pKeys.add("name");

		RowMap rowMap = new RowMap("insert", "MyDatabase", "MyTable", TIMESTAMP_MILLISECONDS, pKeys, POSITION);

		rowMap.putData("id", 9001);
		rowMap.putData("name", "example");
		rowMap.putData("column", "value");

		List<String> pkColumns = rowMap.getPrimaryKeyColumns();
		Assert.assertEquals("id", pkColumns.get(0));
		Assert.assertEquals("name", pkColumns.get(1));
	}

	@Test
	public void testGetPrimaryKeyMap() {
		List<String> pKeys = new ArrayList<>();

		pKeys.add("id");

		pKeys.add("name");

		RowMap rowMap = new RowMap("insert", "MyDatabase", "MyTable", TIMESTAMP_MILLISECONDS, pKeys, POSITION);

		rowMap.putData("id", 9001);
		rowMap.putData("name", "example");
		rowMap.putData("column", "value");

		Map<String, Object> pkMap = rowMap.getPrimaryKeyMap();
		Assert.assertEquals(9001, pkMap.get("id"));
		Assert.assertEquals("example", pkMap.get("name"));
	}

	@Test
	public void testGetPrimaryKeyMapWithoutData() {
		List<String> pKeys = new ArrayList<>();

		pKeys.add("id");
		pKeys.add("name");

		RowMap rowMap = new RowMap("delete", "MyDatabase", "MyTable", TIMESTAMP_MILLISECONDS, pKeys, POSITION);

		rowMap.putOldData("id", 9001);
		rowMap.putOldData("name", "example");
		rowMap.putOldData("column", "value");

		Map<String, Object> pkMap = rowMap.getPrimaryKeyMap();
		Assert.assertEquals(null, pkMap.get("id"));
		Assert.assertEquals(null, pkMap.get("name"));
	}

	@Test
	public void testGetRowIdentity() {
		List<String> pKeys = new ArrayList<>();

		pKeys.add("id");

		pKeys.add("name");

		RowMap rowMap = new RowMap("insert", "MyDatabase", "MyTable", TIMESTAMP_MILLISECONDS, pKeys, POSITION);

		rowMap.putData("id", 9001);
		rowMap.putData("name", "example");

		RowIdentity pk = rowMap.getRowIdentity();
		Assert.assertEquals("9001example", pk.toConcatString());
	}

	@Test
	public void testBuildPartitionKey() {
		List<String> pKeys = new ArrayList<>();

		pKeys.add("id");

		pKeys.add("first_name");

		RowMap rowMap = new RowMap("insert", "MyDatabase", "MyTable", TIMESTAMP_MILLISECONDS, pKeys, POSITION);

		rowMap.putData("id", 9001);
		rowMap.putData("first_name", "foo");
		rowMap.putData("middle_name", "buzz");
		rowMap.putData("last_name", "bar");
		rowMap.putData("salary", "4000");
		rowMap.putData("department", "science");

		List<String> partitionColumns = Arrays.asList("id", "first_name", "middle_name", "last_name", "salary", "department");

		String partitionKey = rowMap.buildPartitionKey(partitionColumns);

		Assert.assertEquals("9001foobuzzbar4000science", partitionKey);

	}

	@Test
	public void testBuildPartitionKeyWithEmptyList() {

		List<String> pKeys = new ArrayList<>();

		pKeys.add("id");

		pKeys.add("first_name");

		RowMap rowMap = new RowMap("insert", "MyDatabase", "MyTable", TIMESTAMP_MILLISECONDS, pKeys, POSITION);

		rowMap.putData("id", 9001);
		rowMap.putData("first_name", "foo");
		rowMap.putData("middle_name", "buzz");
		rowMap.putData("last_name", "bar");
		rowMap.putData("salary", "4000");
		rowMap.putData("department", "science");

		List<String> partitionColumns = new ArrayList<>();

		String partitionKey = rowMap.buildPartitionKey(partitionColumns);

		Assert.assertEquals("", partitionKey);

	}

	@Test
	public void testToJSONWithRawJSONData() throws Exception {
		RowMap rowMap = new RowMap("insert", "MyDatabase", "MyTable", TIMESTAMP_MILLISECONDS,
				Arrays.asList("id", "first_name"), POSITION);

		rowMap.setServerId(7653213L);
		rowMap.setThreadId(6532312L);
		rowMap.setSchemaId(298L);

		rowMap.putExtraAttribute("int", 1234);
		rowMap.putExtraAttribute("str", "foo");

		rowMap.putData("id", 9001);
		rowMap.putData("first_name", "foo");
		rowMap.putData("last_name", "bar");
		rowMap.putData("rawJSON", new RawJSONString("{\"UserID\":20}"));

		MaxwellOutputConfig outputConfig = getMaxwellOutputConfig();

		Assert.assertEquals("{\"database\":\"MyDatabase\",\"table\":\"MyTable\",\"type\":\"insert\"," +
				"\"ts\":1496712943,\"position\":\"binlog-0001:1\",\"gtid\":null,\"server_id\":7653213," +
				"\"thread_id\":6532312,\"schema_id\":298,\"int\":1234,\"str\":\"foo\",\"primary_key\":[9001,\"foo\"]," +
				"\"primary_key_columns\":[\"id\",\"first_name\"],\"data\":" + "{\"id\":9001,\"first_name\":\"foo\"," +
				"\"last_name\":\"bar\",\"rawJSON\":{\"UserID\":20}}}",
				rowMap.toJSON(outputConfig));

	}

	@Test
	public void testToJSONWithQuery() throws Exception {
		RowMap rowMap = new RowMap("insert", "MyDatabase", "MyTable", TIMESTAMP_MILLISECONDS,
				Arrays.asList("id", "first_name"), POSITION);

		rowMap.setServerId(7653213L);
		rowMap.setThreadId(6532312L);
		rowMap.setSchemaId(298L);

		rowMap.putExtraAttribute("int", 1234);
		rowMap.putExtraAttribute("str", "foo");

		rowMap.putData("id", 9001);
		rowMap.putData("first_name", "foo");
		rowMap.putData("last_name", "bar");
		rowMap.putData("rawJSON", new RawJSONString("{\"UserID\":20}"));

		rowMap.setRowQuery("INSERT INTO MyTable VALUES ('foo','bar')");

		MaxwellOutputConfig outputConfig = getMaxwellOutputConfig();
		outputConfig.includesRowQuery = true;

		Assert.assertEquals("{\"database\":\"MyDatabase\",\"table\":\"MyTable\"," +
				"\"query\":\"INSERT INTO MyTable VALUES ('foo','bar')\",\"type\":\"insert\"," +
				"\"ts\":1496712943,\"position\":\"binlog-0001:1\",\"gtid\":null,\"server_id\":7653213," +
				"\"thread_id\":6532312,\"schema_id\":298,\"int\":1234,\"str\":\"foo\",\"primary_key\":[9001,\"foo\"]," +
				"\"primary_key_columns\":[\"id\",\"first_name\"],\"data\":" + "{\"id\":9001,\"first_name\":\"foo\"," +
				"\"last_name\":\"bar\",\"rawJSON\":{\"UserID\":20}}}",
				rowMap.toJSON(outputConfig));

	}

	@Test
	public void testToJSONWithQueryOverMaxLength() throws Exception {
		RowMap rowMap = new RowMap("insert", "MyDatabase", "MyTable", TIMESTAMP_MILLISECONDS,
				Arrays.asList("id", "first_name"), POSITION);

		rowMap.setServerId(7653213L);
		rowMap.setThreadId(6532312L);
		rowMap.setSchemaId(298L);

		rowMap.putExtraAttribute("int", 1234);
		rowMap.putExtraAttribute("str", "foo");

		rowMap.putData("id", 9001);
		rowMap.putData("first_name", "foo");
		rowMap.putData("last_name", "bar");
		rowMap.putData("rawJSON", new RawJSONString("{\"UserID\":20}"));

		rowMap.setRowQuery("INSERT INTO MyTable VALUES ('foo','bar')");

		MaxwellOutputConfig outputConfig = getMaxwellOutputConfig();
		outputConfig.includesRowQuery = true;
		outputConfig.rowQueryMaxLength = 10;

		Assert.assertEquals("{\"database\":\"MyDatabase\",\"table\":\"MyTable\"," +
				"\"query\":\"INSERT INT\",\"type\":\"insert\"," +
				"\"ts\":1496712943,\"position\":\"binlog-0001:1\",\"gtid\":null,\"server_id\":7653213," +
				"\"thread_id\":6532312,\"schema_id\":298,\"int\":1234,\"str\":\"foo\",\"primary_key\":[9001,\"foo\"]," +
				"\"primary_key_columns\":[\"id\",\"first_name\"],\"data\":" + "{\"id\":9001,\"first_name\":\"foo\"," +
				"\"last_name\":\"bar\",\"rawJSON\":{\"UserID\":20}}}",
				rowMap.toJSON(outputConfig));

	}

	@Test
	public void testToJSONWithListData() throws Exception {
		RowMap rowMap = new RowMap("insert", "MyDatabase", "MyTable", TIMESTAMP_MILLISECONDS,
				Arrays.asList("id", "first_name"), POSITION);

		rowMap.setServerId(7653213L);
		rowMap.setThreadId(6532312L);
		rowMap.setSchemaId(298L);

		rowMap.putExtraAttribute("int", 1234);
		rowMap.putExtraAttribute("str", "foo");

		rowMap.putData("id", 9001);
		rowMap.putData("first_name", "foo");
		rowMap.putData("last_name", "bar");
		rowMap.putData("interests", Arrays.asList("hiking", "programming"));

		MaxwellOutputConfig outputConfig = getMaxwellOutputConfig(Pattern.compile("^.*name.*$"));

		Assert.assertEquals("{\"database\":\"MyDatabase\",\"table\":\"MyTable\",\"type\":\"insert\"," +
				"\"ts\":1496712943,\"position\":\"binlog-0001:1\",\"gtid\":null,\"server_id\":7653213," +
				"\"thread_id\":6532312,\"schema_id\":298,\"int\":1234,\"str\":\"foo\",\"primary_key\":[9001,\"foo\"]," +
				"\"primary_key_columns\":[\"id\",\"first_name\"],\"data\":{\"id\":9001,\"interests\"" +
				":[\"hiking\",\"programming\"]}}", rowMap.toJSON(outputConfig));
	}
	
	@Test
	public void testNamingStrategy() throws Exception {
		RowMap rowMap = new RowMap(	"insert",
									"MyDatabase",
									"MyTable",
									TIMESTAMP_MILLISECONDS,
									Arrays.asList("id", "first_name"),
									POSITION);

		rowMap.setServerId(7653213L);
		rowMap.setThreadId(6532312L);
		rowMap.setSchemaId(298L);

		rowMap.putExtraAttribute("int", 1234);
		rowMap.putExtraAttribute("str", "foo");

		rowMap.putData("id", 9001);
		rowMap.putData("first_name", "foo");
		rowMap.putData("last_name", "bar");
		rowMap.putData("_age_", 12);
		rowMap.putData("_parent_id", 9000);
		rowMap.putData("__grand_parent_id", 8000);
		rowMap.putData("_invalid_!chars", 8001);
		rowMap.putData("_____", 8002);
		rowMap.putData("__123", 8003);
		rowMap.putData("favorite_interests", Arrays.asList("hiking", "programming"));
		rowMap.putData("odd_ҫot_lowercase", 1);

		MaxwellOutputConfig outputConfig = getMaxwellOutputConfig(Pattern.compile("^.*name.*$"));
		outputConfig.namingStrategy = FieldNameStrategy.NAME_UNDERSCORE_TO_CAMEL_CASE;
		{
			Assert.assertEquals("{\"database\":\"MyDatabase\",\"table\":\"MyTable\",\"type\":\"insert\"," +
								"\"ts\":1496712943,\"position\":\"binlog-0001:1\",\"gtid\":null," +
								"\"serverId\":7653213," + // camel case applied
								"\"threadId\":6532312," + // camel case applied
								"\"schemaId\":298," + // camel case applied
								"\"int\":1234,\"str\":\"foo\"," + "\"primaryKey\":[9001,\"foo\"]," + // camel case applied
								"\"primaryKeyColumns\":[\"id\",\"first_name\"]," + // first_name not converted because
																					// it's a value instead of name of a
																					// field
								"\"data\":{\"id\":9001," + "\"age\":12,\"parentId\":9000," +
										"\"grandParentId\":8000," +
								"\"invalid!chars\":8001," + //non-ascii char after underscore keeps the same
								"\"_____\":8002," +		//extreme case, leave the old
								"\"123\":8003," +		//all underscore chars are removed 
								"\"favoriteInterests\":[\"hiking\",\"programming\"]," + // camel case applied
								"\"oddҪotLowercase\":1}}",
								rowMap.toJSON(outputConfig));
		}
		// clear the value for other cases
		outputConfig.namingStrategy = null;
	}

	private MaxwellOutputConfig getMaxwellOutputConfig(Pattern... patterns) {
		MaxwellOutputConfig outputConfig = new MaxwellOutputConfig();

		outputConfig.includesBinlogPosition = true;
		outputConfig.includesCommitInfo = true;
		outputConfig.includesGtidPosition = true;
		outputConfig.includesServerId = true;
		outputConfig.includesThreadId = true;
		outputConfig.includesSchemaId = true;
		outputConfig.includesNulls = true;
		outputConfig.includesPrimaryKeys = true;
		outputConfig.includesPrimaryKeyColumns = true;
		outputConfig.excludeColumns = Arrays.asList(patterns);

		return outputConfig;
	}
}
