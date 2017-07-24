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

	@Test
	public void testEncryptedInsert() throws Exception {
		byte[] bytes = ByteStreams.toByteArray(this.getClass().getResourceAsStream("/json/encrypted-user-insert.json"));
		Assert.assertNotNull(bytes);

		RowMap rowMap = RowMapDeserializer.createFromString(new String(bytes),"aaaaaaaaaaaaaaaa");
		Assert.assertNotNull(rowMap);

		Assert.assertEquals("shard_1", rowMap.getDatabase());
		Assert.assertEquals("minimal", rowMap.getTable());
		Assert.assertEquals("insert", rowMap.getRowType());
		Assert.assertEquals("1500930425", rowMap.getTimestamp().toString());
		Assert.assertEquals("161", rowMap.getXid().toString());
		Assert.assertEquals(1, rowMap.getData("id"));
		Assert.assertEquals(1, rowMap.getData("account_id"));
		Assert.assertEquals("hello", rowMap.getData("text_field"));
	}

	@Test
	public void testEncryptedUpdate() throws Exception {
		byte[] bytes = ByteStreams.toByteArray(this.getClass().getResourceAsStream("/json/encrypted-user-update.json"));
		Assert.assertNotNull(bytes);

		RowMap rowMap = RowMapDeserializer.createFromString(new String(bytes),"aaaaaaaaaaaaaaaa");
		Assert.assertNotNull(rowMap);

		Assert.assertEquals("shard_1", rowMap.getDatabase());
		Assert.assertEquals("minimal", rowMap.getTable());
		Assert.assertEquals("update", rowMap.getRowType());
		Assert.assertEquals("1500934155", rowMap.getTimestamp().toString());
		Assert.assertEquals("294", rowMap.getXid().toString());
		Assert.assertEquals(1, rowMap.getData("id"));
		Assert.assertEquals(1, rowMap.getData("account_id"));
		Assert.assertEquals("goodbye", rowMap.getData("text_field"));
	}

	@Test
	public void testAllEncryptedInsert() throws Exception {
		byte[] bytes = ByteStreams.toByteArray(this.getClass().getResourceAsStream("/json/all-encrypted-user-insert.json"));
		Assert.assertNotNull(bytes);

		RowMap rowMap = RowMapDeserializer.createFromString(new String(bytes),"aaaaaaaaaaaaaaaa");
		Assert.assertNotNull(rowMap);

		Assert.assertEquals("shard_1", rowMap.getDatabase());
		Assert.assertEquals("minimal", rowMap.getTable());
		Assert.assertEquals("insert", rowMap.getRowType());
		Assert.assertEquals("1500935709", rowMap.getTimestamp().toString());
		Assert.assertEquals("161", rowMap.getXid().toString());
		Assert.assertEquals(1, rowMap.getData("id"));
		Assert.assertEquals(1, rowMap.getData("account_id"));
		Assert.assertEquals("hello", rowMap.getData("text_field"));
	}

	@Test
	public void testAllEncryptedUpdate() throws Exception {
		byte[] bytes = ByteStreams.toByteArray(this.getClass().getResourceAsStream("/json/all-encrypted-user-update.json"));
		Assert.assertNotNull(bytes);

		RowMap rowMap = RowMapDeserializer.createFromString(new String(bytes),"aaaaaaaaaaaaaaaa");
		Assert.assertNotNull(rowMap);

		Assert.assertEquals("shard_1", rowMap.getDatabase());
		Assert.assertEquals("minimal", rowMap.getTable());
		Assert.assertEquals("update", rowMap.getRowType());
		Assert.assertEquals("1500935710", rowMap.getTimestamp().toString());
		Assert.assertEquals("294", rowMap.getXid().toString());
		Assert.assertEquals(1, rowMap.getData("id"));
		Assert.assertEquals(1, rowMap.getData("account_id"));
		Assert.assertEquals("goodbye", rowMap.getData("text_field"));
	}
}
