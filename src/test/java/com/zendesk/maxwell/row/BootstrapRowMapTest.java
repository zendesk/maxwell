package com.zendesk.maxwell.row;

import java.sql.Timestamp;

import org.junit.Assert;
import org.junit.Test;

import com.zendesk.maxwell.bootstrap.BootstrapPoller.BootstrapEntry;
import com.zendesk.maxwell.replication.BinlogPosition;
import com.zendesk.maxwell.replication.Position;

public class BootstrapRowMapTest {
	@Test
	public void testGetDataMaps() throws Exception {
		BootstrapEntry entry = new BootstrapEntry();
		entry.id = 1;
		entry.database = "test_database";
		entry.table = "test_table";
		entry.where = null;
		entry.complete = false;
		entry.inserted_rows = 0;
		entry.total_rows = 0;
		entry.created_at = Timestamp.valueOf("2017-01-01 00:00:00");
		entry.started_at = Timestamp.valueOf("2017-01-01 00:00:00");
		entry.completed_at = null;
		entry.binlog_file = null;
		entry.binlog_position = 0;

		BootstrapRowMap rowMap = new BootstrapRowMap("insert", "maxwell", entry,
				new Position(BinlogPosition.at(0, "a"), 0L));

		// Sanity check.
		Assert.assertEquals(1L, rowMap.getData("id"));
		Assert.assertEquals("test_database", rowMap.getData("database_name"));
		Assert.assertEquals("test_table", rowMap.getData("table_name"));
		Assert.assertEquals(null, rowMap.getData("where_clause"));
		Assert.assertEquals(0L, rowMap.getData("is_complete"));
		Assert.assertEquals(0L, rowMap.getData("inserted_rows"));
		Assert.assertEquals(0L, rowMap.getData("total_rows"));
		Assert.assertEquals("2017-01-01", rowMap.getData("created_at"));
		Assert.assertEquals("2017-01-01", rowMap.getData("started_at"));
		Assert.assertEquals(null, rowMap.getData("completed_at"));
		Assert.assertEquals(null, rowMap.getData("binlog_file"));
		Assert.assertEquals(0L, rowMap.getData("binlog_position"));
	}
}
