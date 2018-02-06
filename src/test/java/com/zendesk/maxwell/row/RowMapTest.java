package com.zendesk.maxwell.row;

import com.zendesk.maxwell.MaxwellTestJSON;
import com.zendesk.maxwell.producer.MaxwellOutputConfig;
import com.zendesk.maxwell.replication.BinlogPosition;
import com.zendesk.maxwell.replication.Position;
import com.zendesk.maxwell.schema.columndef.ColumnDef;
import com.zendesk.maxwell.schema.columndef.IntColumnDef;
import com.zendesk.maxwell.schema.columndef.StringColumnDef;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;

public class RowMapTest {

	private static final long TIMESTAMP_MILLISECONDS = 1496712943447L;

	private static final Position POSITION = new Position(new BinlogPosition(1L, "binlog-0001"), 0L);


	@Test
	public void testGetDataMaps() throws Exception {
		RowMap rowMap = new RowMap("insert", "MyDatabase", "MyTable", 1234567890L, new ArrayList<String>(), null);
		rowMap.putData("foo", "bar", ColumnDef.build("foo", "utf-8", "varchar", 0, false, null, 100L));
		rowMap.putData("foo2", 4294967290L, ColumnDef.build("foo2", null, "int", 1, true, null, null));
		rowMap.putOldData("fiz", "buz", ColumnDef.build("fiz", "utf-8", "varchar", 2, false, null, 100L));

		// Sanity check.
		Assert.assertEquals("bar", rowMap.getData("foo"));
		Assert.assertEquals(4294967290L, rowMap.getData("foo2"));
		Assert.assertEquals("buz", rowMap.getOldData("fiz"));

		Assert.assertTrue(rowMap.getDataColumnDef("foo") instanceof StringColumnDef);
		Assert.assertTrue(rowMap.getDataColumnDef("foo2") instanceof IntColumnDef);
		Assert.assertTrue(rowMap.getOldDataColumnDef("fiz") instanceof StringColumnDef);

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
		long timestampSeconds = 1496712943;

		RowMap rowMap = new RowMap("insert", "MyDatabase", "MyTable", TIMESTAMP_MILLISECONDS, new ArrayList<String>(), POSITION);

		Assert.assertEquals(timestampSeconds, rowMap.getTimestamp().longValue());
		Assert.assertEquals(TIMESTAMP_MILLISECONDS, rowMap.getTimestampMillis().longValue());
		Map<String, Object> output = MaxwellTestJSON.parseJSON(rowMap.toJSON());

		int ts = (int) output.get("ts");
		Assert.assertEquals(ts, timestampSeconds);
	}

	@Test
	public void testPkToJsonHash() throws IOException {

		List<String> pKeys = new ArrayList<>();

		pKeys.add("id");

		pKeys.add("name");

		RowMap rowMap = new RowMap("insert", "MyDatabase", "MyTable", TIMESTAMP_MILLISECONDS, pKeys, POSITION);

		rowMap.putData("id", "9001");
		rowMap.putData("name", "example");

		String jsonString = rowMap.pkToJson(RowMap.KeyFormat.HASH);

		Assert.assertEquals("{\"database\":\"MyDatabase\",\"table\":\"MyTable\",\"pk.id\":\"9001\",\"pk.name\":\"example\"}",
				jsonString);


	}


	@Test
	public void testPkToJsonHashWithEmptyData() throws Exception {

		RowMap rowMap = new RowMap("insert", "MyDatabase", "MyTable", TIMESTAMP_MILLISECONDS, new ArrayList<String>(), POSITION);

		String jsonString = rowMap.pkToJson(RowMap.KeyFormat.HASH);

		Map<String, Object> jsonMap = MaxwellTestJSON.parseJSON(jsonString);

		Assert.assertTrue(jsonMap.containsKey("_uuid"));
		Assert.assertEquals("MyDatabase", jsonMap.get("database"));
		Assert.assertEquals("MyTable", jsonMap.get("table"));


	}

	@Test
	public void testPkToJsonArray() throws IOException {

		List<String> pKeys = new ArrayList<>();

		pKeys.add("id");

		pKeys.add("name");

		Position position = new Position(new BinlogPosition(1L, "binlog-0001"), 0L);

		RowMap rowMap = new RowMap("insert", "MyDatabase", "MyTable", TIMESTAMP_MILLISECONDS, pKeys, position);

		rowMap.putData("id", "9001");
		rowMap.putData("name", "example");

		String jsonString = rowMap.pkToJson(RowMap.KeyFormat.ARRAY);

		Assert.assertEquals("[\"MyDatabase\",\"MyTable\",[{\"id\":\"9001\"},{\"name\":\"example\"}]]",
				jsonString);


	}

	@Test
	public void testBuildPartitionKey() {
		List<String> pKeys = new ArrayList<>();

		pKeys.add("id");

		pKeys.add("first_name");

		RowMap rowMap = new RowMap("insert", "MyDatabase", "MyTable", TIMESTAMP_MILLISECONDS, pKeys, POSITION);

		rowMap.putData("id", "9001");
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

		rowMap.putData("id", "9001");
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
				new ArrayList<String>(), POSITION);

		rowMap.setServerId(7653213L);
		rowMap.setThreadId(6532312L);

		rowMap.putData("id", "9001");
		rowMap.putData("first_name", "foo");
		rowMap.putData("last_name", "bar");
        rowMap.putData("rawJSON", new RawJSONString("{\"UserID\":20}"));
		MaxwellOutputConfig outputConfig = getMaxwellOutputConfig();

		Assert.assertEquals("{\"database\":\"MyDatabase\",\"table\":\"MyTable\",\"type\":\"insert\"," +
				"\"ts\":1496712943,\"position\":\"binlog-0001:1\",\"gtid\":null,\"server_id\":7653213,\"thread_id\":6532312," +
				"\"data\":{\"id\":\"9001\",\"first_name\":\"foo\",\"last_name\":\"bar\",\"rawJSON\":{\"UserID\":20}}}", rowMap.toJSON(outputConfig));

	}

	@Test
	public void testToJSONWithListData() throws Exception {
		RowMap rowMap = new RowMap("insert", "MyDatabase", "MyTable", TIMESTAMP_MILLISECONDS,
				new ArrayList<String>(), POSITION);

		rowMap.putData("id", "9001");
		rowMap.putData("first_name", "foo");
		rowMap.putData("last_name", "bar");
		rowMap.putData("interests", Arrays.asList("hiking", "programming"));
		rowMap.setServerId(7653213L);
		rowMap.setThreadId(6532312L);
		MaxwellOutputConfig outputConfig = getMaxwellOutputConfig(Pattern.compile("^.*name.*$"));

		Assert.assertEquals("{\"database\":\"MyDatabase\",\"table\":\"MyTable\",\"type\":\"insert\",\"ts\":1496712943," +
				"\"position\":\"binlog-0001:1\",\"gtid\":null,\"server_id\":7653213,\"thread_id\":6532312," +
				"\"data\":{\"id\":\"9001\",\"interests\":[\"hiking\",\"programming\"]}}", rowMap.toJSON(outputConfig));
	}

	private MaxwellOutputConfig getMaxwellOutputConfig(Pattern... patterns) {
		MaxwellOutputConfig outputConfig = new MaxwellOutputConfig();

		outputConfig.includesBinlogPosition = true;
		outputConfig.includesCommitInfo = true;
		outputConfig.includesGtidPosition = true;
		outputConfig.includesServerId = true;
		outputConfig.includesThreadId = true;
		outputConfig.includesNulls = true;
		outputConfig.excludeColumns = Arrays.asList(patterns);

		return outputConfig;
	}
}
