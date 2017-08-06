package com.zendesk.maxwell.row;

import com.fasterxml.jackson.databind.node.BigIntegerNode;
import com.fasterxml.jackson.databind.node.DecimalNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.FloatNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.ValueNode;
import com.google.common.io.ByteStreams;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;

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
		Assert.assertTrue(rowMap.isTXCommit());
		Assert.assertEquals("1486439516", rowMap.getTimestamp().toString());
		Assert.assertEquals("5694", rowMap.getXid().toString());
		Assert.assertEquals(20, rowMap.getData("UserID"));
		Assert.assertEquals("Fiz", rowMap.getData("FirstName"));
		Assert.assertNull(rowMap.getData("MiddleName"));
		Assert.assertEquals("Buz", rowMap.getData("LastName"));
		Assert.assertEquals(1486703131000L, rowMap.getData("Version"));
		Assert.assertEquals(4500000001.512456777d, (double)rowMap.getData("DoubleField"), .000000000001d);
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
		Assert.assertTrue(rowMap.isTXCommit());
		Assert.assertEquals("1486439516", rowMap.getTimestamp().toString());
		Assert.assertEquals("5694", rowMap.getXid().toString());
		Assert.assertEquals(20, rowMap.getData("UserID"));
		Assert.assertEquals("Fiz", rowMap.getData("FirstName"));
		Assert.assertNull(rowMap.getData("MiddleName"));
		Assert.assertEquals("Buz", rowMap.getData("LastName"));
		Assert.assertEquals(1486703131000L, rowMap.getData("Version"));
		Assert.assertEquals("Foo", rowMap.getOldData("FirstName"));
		Assert.assertEquals("Bar", rowMap.getOldData("LastName"));
		Assert.assertEquals(4500000001.512456777d, (double)rowMap.getOldData("DoubleField"), .000000000001d);
	}


	@Test
	public void getValue_allValueNodeNumberTypes_assertCorrectValue()
	{
		//Arrange
		RowMapDeserializer deserializer = new RowMapDeserializer();

		ValueNode doubleNode = new DoubleNode(234.32456666d);
		ValueNode bigIntegerNode = new BigIntegerNode(new BigInteger("234234234"));
		ValueNode bigDecimalNode = new DecimalNode(new BigDecimal("234234234.32"));
		ValueNode floatNode = new FloatNode(234.32456666f);
		ValueNode intNode = new IntNode(1213332);
		ValueNode longNode = new LongNode(1213332333212L);

		//Act
		Object doubleNodeValue = deserializer.getValue(doubleNode);
		Object bigIntegerNodeValue = deserializer.getValue(bigIntegerNode);
		Object bigDecimalNodeValue = deserializer.getValue(bigDecimalNode);
		Object floatNodeValue = deserializer.getValue(floatNode);
		Object intNodeValue = deserializer.getValue(intNode);
		Object longNodeValue = deserializer.getValue(longNode);

		//Assert
		Assert.assertEquals(doubleNodeValue, doubleNode.doubleValue());
		Assert.assertTrue(doubleNodeValue.getClass() == Double.class);

		Assert.assertEquals(bigIntegerNodeValue, bigIntegerNode.bigIntegerValue());
		Assert.assertTrue(bigIntegerNodeValue.getClass() == BigInteger.class);

		Assert.assertEquals(bigDecimalNodeValue, bigDecimalNode.decimalValue());
		Assert.assertTrue(bigDecimalNodeValue.getClass() == BigDecimal.class);

		Assert.assertEquals(floatNodeValue, floatNode.floatValue());
		Assert.assertTrue(floatNodeValue.getClass() == Float.class);

		Assert.assertEquals(intNodeValue, intNode.intValue());
		Assert.assertTrue(intNodeValue.getClass() == Integer.class);

		Assert.assertEquals(longNodeValue, longNode.longValue());
		Assert.assertTrue(longNodeValue.getClass() == Long.class);
	}




}
