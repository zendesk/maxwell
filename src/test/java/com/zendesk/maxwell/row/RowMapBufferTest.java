package com.zendesk.maxwell.row;

import com.zendesk.maxwell.TestWithNameLogging;
import com.zendesk.maxwell.replication.BinlogPosition;
import com.zendesk.maxwell.replication.Position;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;

public class RowMapBufferTest extends TestWithNameLogging {
	@Test
	public void TestOverflowToDisk() throws Exception {
		RowMapBuffer buffer = new RowMapBuffer(2, 250); // allow about 250 bytes of memory to be used

		RowMap r;
		buffer.add(new RowMap("insert", "foo", "bar", 1000L, new ArrayList<String>(), new Position(new BinlogPosition(3, "mysql.1"), 0L)));
		buffer.add(new RowMap("insert", "foo", "bar", 2000L, new ArrayList<String>(), new Position(new BinlogPosition(3, "mysql.1"), 0L)));
		buffer.add(new RowMap("insert", "foo", "bar", 3000L, new ArrayList<String>(), new Position(new BinlogPosition(3, "mysql.1"), 0L)));

		assertThat(buffer.size(), is(3L));
		assertThat(buffer.inMemorySize(), is(2L));

		assertThat(buffer.removeFirst().getTimestamp(), is(1L));
		assertThat(buffer.removeFirst().getTimestamp(), is(2L));
		assertThat(buffer.removeFirst().getTimestamp(), is(3L));
	}

	@Test
	public void TestXOffsetIncrement() throws IOException, ClassNotFoundException {
		RowMapBuffer buffer = new RowMapBuffer(100);

		buffer.add(new RowMap("insert", "foo", "bar", 1000L, new ArrayList<String>(), new Position(new BinlogPosition(3, "mysql.1"), 0L)));
		buffer.add(new RowMap("insert", "foo", "bar", 2000L, new ArrayList<String>(), new Position(new BinlogPosition(3, "mysql.1"), 0L)));
		buffer.add(new RowMap("insert", "foo", "bar", 3000L, new ArrayList<String>(), new Position(new BinlogPosition(3, "mysql.1"), 0L)));

		assert buffer.removeFirst().getXoffset() == 0;
		assert buffer.removeFirst().getXoffset() == 1;
		assert buffer.removeFirst().getXoffset() == 2;
		assert buffer.isEmpty();

		buffer.add(new RowMap("insert", "foo", "bar", 3000L, new ArrayList<String>(), new Position(new BinlogPosition(3, "mysql.1"), 0L)));
		assert buffer.removeFirst().getXoffset() == 3;

		buffer = new RowMapBuffer(100);
		buffer.add(new RowMap("insert", "foo", "bar", 1000L, new ArrayList<String>(), new Position(new BinlogPosition(3, "mysql.1"), 0L)));
		assert buffer.removeFirst().getXoffset() == 0;
	}

	// https://github.com/zendesk/maxwell/issues/996
	@Test
	public void TestOverflowToDiskWithJson() throws Exception {
		RowMapBuffer buffer = new RowMapBuffer(2, 250); // allow about 250 bytes of memory to be used

		RowMap r = new RowMap("insert", "foo", "bar", 1000L, new ArrayList<String>(), new Position(new BinlogPosition(3, "mysql.1"), 0L));
		r.putData("json", new RawJSONString("1"));
		buffer.add(r);
		buffer.add(new RowMap("insert", "foo", "bar", 2000L, new ArrayList<String>(), new Position(new BinlogPosition(3, "mysql.1"), 0L)));
		buffer.add(new RowMap("insert", "foo", "bar", 3000L, new ArrayList<String>(), new Position(new BinlogPosition(3, "mysql.1"), 0L)));

		assertThat(buffer.size(), is(3L));
		assertThat(buffer.inMemorySize(), is(2L));

		assertThat(buffer.removeFirst().getTimestamp(), is(1L));
		assertThat(buffer.removeFirst().getTimestamp(), is(2L));
		assertThat(buffer.removeFirst().getTimestamp(), is(3L));
	}
}
