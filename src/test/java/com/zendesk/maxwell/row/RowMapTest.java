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
import java.util.Collections;
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
