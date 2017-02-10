package com.zendesk.maxwell.row;

import com.google.common.io.ByteStreams;
import org.junit.Assert;
import org.junit.Test;

public class RowMapDeserializerTest {
	@Test
	public void testInsert() throws Exception {
		byte[] bytes = ByteStreams.toByteArray(this.getClass().getResourceAsStream("/json/user-insert.json"));
		Assert.assertNotNull(bytes);

		RowMap rowMap = RowMapDeserializer.createFromString(new String(bytes));
		Assert.assertNotNull(rowMap);

		Assert.assertEquals("MyDatabase", rowMap.getDatabase());
		Assert.assertEquals("User", rowMap.getTable());
		Assert.assertEquals("insert", rowMap.getRowType());
		Assert.assertEquals("1486439516", rowMap.getTimestamp().toString());
		Assert.assertEquals("5694", rowMap.getXid().toString());
		Assert.assertEquals(20, rowMap.getData("UserID"));
		Assert.assertEquals("Fiz", rowMap.getData("FirstName"));
		Assert.assertNull(rowMap.getData("MiddleName"));
		Assert.assertEquals("Buz", rowMap.getData("LastName"));
		Assert.assertEquals(1486703131000L, rowMap.getData("Version"));
	}

	@Test
	public void testUpdate() throws Exception {
		byte[] bytes = ByteStreams.toByteArray(this.getClass().getResourceAsStream("/json/user-update.json"));
		Assert.assertNotNull(bytes);

		RowMap rowMap = RowMapDeserializer.createFromString(new String(bytes));
		Assert.assertNotNull(rowMap);

		Assert.assertEquals("MyDatabase", rowMap.getDatabase());
		Assert.assertEquals("User", rowMap.getTable());
		Assert.assertEquals("update", rowMap.getRowType());
		Assert.assertEquals("1486439516", rowMap.getTimestamp().toString());
		Assert.assertEquals("5694", rowMap.getXid().toString());
		Assert.assertEquals(20, rowMap.getData("UserID"));
		Assert.assertEquals("Fiz", rowMap.getData("FirstName"));
		Assert.assertNull(rowMap.getData("MiddleName"));
		Assert.assertEquals("Buz", rowMap.getData("LastName"));
		Assert.assertEquals(1486703131000L, rowMap.getData("Version"));
		Assert.assertEquals("Foo", rowMap.getOldData("FirstName"));
		Assert.assertEquals("Bar", rowMap.getOldData("LastName"));
	}
}
