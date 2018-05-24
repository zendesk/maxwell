package com.zendesk.maxwell.row;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;

public class RowMapBufferByTable {

	private final long MAX_TX_ELEMENTS = 10000;

	private class Buffer extends RowMapBuffer {
		public Buffer() throws IOException {
			super(MAX_TX_ELEMENTS);
		}
	}

	private HashMap<String, Buffer> buffers = new LinkedHashMap<>();

	public void add(RowMap row) throws IOException {
		getBuffer(row).add(row);
	}

	public RowMap removeFirst(String databaseName, String tableName) throws IOException, ClassNotFoundException {
		return getBuffer(databaseName, tableName).removeFirst();
	}

	public Long size(String databaseName, String tableName) throws IOException {
		return getBuffer(databaseName, tableName).size();
	}

	public void flushToDisk(String databaseName, String tableName) throws IOException {
		getBuffer(databaseName, tableName).flushToDisk();
	}

	private Buffer getBuffer(RowMap row) throws IOException {
		return getBuffer(getKey(row));
	}

	private Buffer getBuffer(String databaseName, String tableName) throws IOException {
		return getBuffer(getKey(databaseName, tableName));
	}

	private Buffer getBuffer(String key) throws IOException {
		Buffer buffer = buffers.get(key);
		if (buffer == null) {
			buffer = new Buffer();
			buffers.put(key, buffer);
		}
		return buffer;
	}

	private String getKey(RowMap row) {
		return getKey(row.getDatabase(), row.getTable());
	}

	private String getKey(String databaseName, String tableName) {
		return databaseName + " " + tableName;
	}

}
