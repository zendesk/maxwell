package com.zendesk.maxwell.row;

import com.zendesk.maxwell.MaxwellTestJSON;
import com.zendesk.maxwell.replication.BinlogPosition;
import com.zendesk.maxwell.replication.Position;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

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
}
