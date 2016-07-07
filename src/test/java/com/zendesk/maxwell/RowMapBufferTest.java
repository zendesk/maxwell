package com.zendesk.maxwell;

import org.junit.Test;

import java.util.ArrayList;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;

public class RowMapBufferTest {
	@Test
	public void TestOverflowToDisk() throws Exception {
		RowMapBuffer buffer = new RowMapBuffer(2);

		RowMap r;
		buffer.add(new RowMap("insert", "foo", "bar", 1L, new ArrayList<String>(), new BinlogPosition(3, "mysql.1")));
		buffer.add(new RowMap("insert", "foo", "bar", 2L, new ArrayList<String>(), new BinlogPosition(3, "mysql.1")));
		buffer.add(new RowMap("insert", "foo", "bar", 3L, new ArrayList<String>(), new BinlogPosition(3, "mysql.1")));

		assertThat(buffer.size(), is(3L));
		assertThat(buffer.inMemorySize(), is(2L));

		assertThat(buffer.removeFirst().getTimestamp(), is(1L));
		assertThat(buffer.removeFirst().getTimestamp(), is(2L));
		assertThat(buffer.removeFirst().getTimestamp(), is(3L));
	}
}
