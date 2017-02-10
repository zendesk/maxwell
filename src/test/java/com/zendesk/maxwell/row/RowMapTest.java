package com.zendesk.maxwell.row;

import org.junit.Assert;
import org.junit.Test;

import java.util.LinkedHashMap;

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
}
