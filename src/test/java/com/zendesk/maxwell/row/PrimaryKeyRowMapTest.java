package com.zendesk.maxwell.row;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class PrimaryKeyRowMapTest {

	@Test
	public void testToJsonHash() throws IOException {
		Map<String, Object> kvs = new HashMap<>();
		kvs.put("id", 111);
		PrimaryKeyRowMap keyRowMap = new PrimaryKeyRowMap("MyDatabase", "MyTable", kvs);

		String jsonString = keyRowMap.toJsonHash();

		Assert.assertEquals("{\"database\":\"MyDatabase\",\"table\":\"MyTable\",\"data\":{\"id\":111}}",
				jsonString);
	}

	@Test
	public void testToJsonHashWithReason() throws IOException {
		Map<String, Object> kvs = new HashMap<>();
		kvs.put("id", 111);
		PrimaryKeyRowMap keyRowMap = new PrimaryKeyRowMap("MyDatabase", "MyTable", kvs);

		String jsonString = keyRowMap.toJsonHashWithReason("too big");

		Assert.assertEquals("{\"database\":\"MyDatabase\",\"table\":\"MyTable\",\"reason\":\"too big\",\"data\":{\"id\":111}}",
				jsonString);
	}
}
