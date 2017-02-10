package com.zendesk.maxwell.row;

import com.google.common.io.ByteStreams;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import java.util.LinkedHashMap;

import static org.junit.Assert.assertEquals;

public class RowMapTest {
  private JSONObject json = new JSONObject();

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
  public void testGetType() throws JSONException {
    json.put("type", "patch");

    RowMap result = new RowMap(json);
    assertEquals("patch", result.getRowType());
  }

  @Test
  public void testGetDatabase() throws JSONException {
    json.put("database", "MyDatabase");

    RowMap result = new RowMap(json);
    assertEquals("MyDatabase", result.getDatabase());
  }

  @Test
  public void testGetTable() throws JSONException {
    json.put("table", "MyTable");

    RowMap result = new RowMap(json);
    assertEquals("MyTable", result.getTable());
  }

  @Test
  public void testGetTimestamp() throws JSONException {
    json.put("ts", 1234567890L);

    RowMap result = new RowMap(json);
    assertEquals(String.valueOf(1234567890L), String.valueOf(result.getTimestamp()));
  }

  @Test
  public void testGetXID() throws JSONException {
    json.put("xid", 1234567890L);

    RowMap result = new RowMap(json);
    assertEquals(String.valueOf(1234567890L), String.valueOf(result.getXid()));
  }

  @Test
  public void testGetData() throws JSONException {
    JSONObject data = new JSONObject();
    data.put("FirstName", "Fiz");
    data.put("LastName", "Buz");
    json.put("data", data);

    RowMap result = new RowMap(json);
    assertEquals("Fiz", result.getData("FirstName"));
    assertEquals("Buz", result.getData("LastName"));
  }

  @Test
  public void testGetOldData() throws JSONException {
    JSONObject oldData = new JSONObject();
    oldData.put("FirstName", "Foo");
    oldData.put("LastName", "Bar");
    json.put("old", oldData);

    RowMap result = new RowMap(json);
    assertEquals("Foo", result.getOldData("FirstName"));
    assertEquals("Bar", result.getOldData("LastName"));
  }

  @Test
  public void testInsertIntegrationTest() throws Exception {
    byte[] bytes = ByteStreams.toByteArray(this.getClass().getResourceAsStream("/json/user-insert.json"));
    Assert.assertNotNull(bytes);

    RowMap rowMap = new RowMap(new JSONObject(new String(bytes)));
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
  public void testUpdateIntegrationTest() throws Exception {
    byte[] bytes = ByteStreams.toByteArray(this.getClass().getResourceAsStream("/json/user-update.json"));
    Assert.assertNotNull(bytes);

    RowMap rowMap = new RowMap(new JSONObject(new String(bytes)));
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
