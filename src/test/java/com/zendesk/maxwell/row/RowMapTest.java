package com.zendesk.maxwell.row;

import com.zendesk.maxwell.MaxwellTestJSON;
import com.zendesk.maxwell.producer.MaxwellOutputConfig;
import com.zendesk.maxwell.replication.BinlogPosition;
import com.zendesk.maxwell.replication.Position;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;

public class RowMapTest {
	@Test
	public void testGetDataMaps() throws Exception {
		RowMap rowMap = new RowMap("insert", "MyDatabase", "MyTable", 1234567890L, null, null);
		rowMap.putData("foo", "bar");
		rowMap.putOldData("fiz", "buz");

		// Sanity check.
		Assert.assertEquals("bar", rowMap.getData("foo"));
		Assert.assertEquals("buz", rowMap.getOldData("fiz"));

		// Get data maps.
		LinkedHashMap<String, Object> data = rowMap.getData();
		LinkedHashMap<String, Object> oldData = rowMap.getOldData();
		Assert.assertEquals("bar", data.get("foo"));
		Assert.assertEquals("buz", oldData.get("fiz"));

		// Manipulate data maps extracted from RowMap.
		data.put("foo", "BAR");
		oldData.put("fiz", "BUZ");

		// Another sanity check.
		Assert.assertEquals("BAR", data.get("foo"));
		Assert.assertEquals("BUZ", oldData.get("fiz"));

		// Assert original RowMap data was not changed.
		Assert.assertEquals("bar", rowMap.getData("foo"));
		Assert.assertEquals("buz", rowMap.getOldData("fiz"));
	}

	@Test
	public void testTimestampConversion() throws Exception {
		long timestampMillis = 1496712943447L;
		long timestampSeconds = 1496712943;

		Position position = new Position(new BinlogPosition(1L, "binlog-0001"), 0L);
		RowMap rowMap = new RowMap("insert", "MyDatabase", "MyTable", timestampMillis, null, position);

		Assert.assertEquals(timestampSeconds, rowMap.getTimestamp().longValue());
		Assert.assertEquals(timestampMillis, rowMap.getTimestampMillis().longValue());
		Map<String, Object> output = MaxwellTestJSON.parseJSON(rowMap.toJSON());

		int ts = (int) output.get("ts");
		Assert.assertEquals(ts, timestampSeconds);
	}

	@Test
	public void testPkToJsonHash() throws IOException {

		long timestampMillis = 1496712943447L;

		List<String> pKeys = new ArrayList<>();

		pKeys.add("id");

		pKeys.add("name");

		Position position = new Position(new BinlogPosition(1L, "binlog-0001"), 0L);

		RowMap rowMap = new RowMap("insert", "MyDatabase", "MyTable", timestampMillis, pKeys, position);

		rowMap.putData("id", "9001");
		rowMap.putData("name", "example");

		String jsonString = rowMap.pkToJson(RowMap.KeyFormat.HASH);

		Assert.assertEquals("{\"database\":\"MyDatabase\",\"table\":\"MyTable\",\"pk.id\":\"9001\",\"pk.name\":\"example\"}",
				jsonString);


	}


	@Test
	public void testPkToJsonHashWithEmptyData() throws Exception {

		long timestampMillis = 1496712943447L;

		Position position = new Position(new BinlogPosition(1L, "binlog-0001"), 0L);

		RowMap rowMap = new RowMap("insert", "MyDatabase", "MyTable", timestampMillis, null, position);


		String jsonString = rowMap.pkToJson(RowMap.KeyFormat.HASH);

		Map<String, Object> jsonMap = MaxwellTestJSON.parseJSON(jsonString);

		Assert.assertTrue(jsonMap.containsKey("_uuid"));
		Assert.assertEquals("MyDatabase", jsonMap.get("database"));
		Assert.assertEquals("MyTable", jsonMap.get("table"));


	}

	@Test
	public void testPkToJsonArray() throws IOException {

		long timestampMillis = 1496712943447L;

		List<String> pKeys = new ArrayList<>();

		pKeys.add("id");

		pKeys.add("name");

		Position position = new Position(new BinlogPosition(1L, "binlog-0001"), 0L);

		RowMap rowMap = new RowMap("insert", "MyDatabase", "MyTable", timestampMillis, pKeys, position);

		rowMap.putData("id", "9001");
		rowMap.putData("name", "example");

		String jsonString = rowMap.pkToJson(RowMap.KeyFormat.ARRAY);

		Assert.assertEquals("[\"MyDatabase\",\"MyTable\",[{\"id\":\"9001\"},{\"name\":\"example\"}]]",
				jsonString);


	}

	@Test
	public void testBuildPartitionKey() {
		long timestampMillis = 1496712943447L;

		List<String> pKeys = new ArrayList<>();

		pKeys.add("id");

		pKeys.add("first_name");

		Position position = new Position(new BinlogPosition(1L, "binlog-0001"), 0L);

		RowMap rowMap = new RowMap("insert", "MyDatabase", "MyTable", timestampMillis, pKeys, position);

		rowMap.putData("id", "9001");
		rowMap.putData("first_name", "foo");
		rowMap.putData("middle_name", "buzz");
		rowMap.putData("last_name", "bar");
		rowMap.putData("salary", "4000");
		rowMap.putData("department", "science");

		List<String> partitionColumns = Arrays.asList("id", "first_name", "middle_name", "last_name", "salary", "department");

		String partitionKey = rowMap.buildPartitionKey(partitionColumns, "primary_key");

		Assert.assertEquals("9001foobuzzbar4000science", partitionKey);

	}

	@Test
	public void testBuildPartitionKeyWithEmptyList() {
		long timestampMillis = 1496712943447L;

		List<String> pKeys = new ArrayList<>();

		pKeys.add("id");

		pKeys.add("first_name");

		Position position = new Position(new BinlogPosition(1L, "binlog-0001"), 0L);

		RowMap rowMap = new RowMap("insert", "MyDatabase", "MyTable", timestampMillis, pKeys, position);

		rowMap.putData("id", "9001");
		rowMap.putData("first_name", "foo");
		rowMap.putData("middle_name", "buzz");
		rowMap.putData("last_name", "bar");
		rowMap.putData("salary", "4000");
		rowMap.putData("department", "science");

		List<String> partitionColumns = new ArrayList<>();

		String partitionKey = rowMap.buildPartitionKey(partitionColumns, "primary_key");

		Assert.assertEquals("9001foo", partitionKey);

	}

	@Test
	public void testToJSONWithRawJSONData() throws IOException {
		long timestampMillis = 1496712943487L;

		Position position = new Position(new BinlogPosition(1L, "binlog-0001"), 0L);
		RowMap rowMap = new RowMap("insert", "MyDatabase", "MyTable", timestampMillis,
				null, position);

		rowMap.putData("id", "9001");
		rowMap.putData("first_name", "foo");
		rowMap.putData("middle_name", "buzz");
		rowMap.putData("last_name", "bar");
		rowMap.putData("salary", "4000");
		rowMap.putData("department", "science");
        rowMap.putData("rawJSON", new RawJSONString("{\"database\":\"MyDatabase\",\"table\":\"User\",\"type\":\"insert\",\"ts\":1486439516,\"xid\":5694,\"commit\":true,\"data\":{\"UserID\":20,\"FirstName\":\"Fiz\",\"MiddleName\":null,\"LastName\":\"Buz\",\"Version\":1486703131000}}\n"));
		MaxwellOutputConfig outputConfig = getMaxwellOutputConfig();

		Assert.assertEquals("{\"database\":\"MyDatabase\",\"table\":\"MyTable\",\"type\":\"insert\",\"ts\":1496712943,\"position\":\"binlog-0001:1\",\"gtid\":null,\"data\":{\"id\":\"9001\",\"salary\":\"4000\",\"department\":\"science\",\"rawJSON\":{\"database\":\"MyDatabase\",\"table\":\"User\",\"type\":\"insert\",\"ts\":1486439516,\"xid\":5694,\"commit\":true,\"data\":{\"UserID\":20,\"FirstName\":\"Fiz\",\"MiddleName\":null,\"LastName\":\"Buz\",\"Version\":1486703131000}}\n" +
				"}}", rowMap.toJSON(outputConfig));
	}

	@Test
	public void testToJSONWithListData() throws IOException {
		long timestampMillis = 1496712943487L;

		Position position = new Position(new BinlogPosition(1L, "binlog-0001"), 0L);
		RowMap rowMap = new RowMap("insert", "MyDatabase", "MyTable", timestampMillis,
				null, position);

		rowMap.putData("id", "9001");
		rowMap.putData("first_name", "foo");
		rowMap.putData("middle_name", "buzz");
		rowMap.putData("last_name", "bar");
		rowMap.putData("salary", "4000");
		rowMap.putData("department", "science");
		rowMap.putData("interests", Arrays.asList("classical music", "hiking", "biking", "programming"));
		MaxwellOutputConfig outputConfig = getMaxwellOutputConfig();

		Assert.assertEquals("{\"database\":\"MyDatabase\",\"table\":\"MyTable\",\"type\":\"insert\",\"ts\":1496712943,\"position\":\"binlog-0001:1\",\"gtid\":null,\"data\":{\"id\":\"9001\",\"salary\":\"4000\",\"department\":\"science\",\"interests\":[\"classical music\",\"hiking\",\"biking\",\"programming\"]}}", rowMap.toJSON(outputConfig));
	}

	private MaxwellOutputConfig getMaxwellOutputConfig() {
		MaxwellOutputConfig outputConfig = new MaxwellOutputConfig();



		List<Pattern> patterns = new ArrayList<>();
		Pattern pattern = Pattern.compile("^.*name.*$");
		patterns.add(pattern);

		outputConfig.includesBinlogPosition = true;
		outputConfig.includesCommitInfo = true;
		outputConfig.includesGtidPosition = true;
		outputConfig.includesServerId = true;
		outputConfig.includesThreadId = true;
		outputConfig.includesNulls = true;
		outputConfig.excludeColumns = patterns;

		return outputConfig;

	}
}
