package com.zendesk.maxwell;

import java.util.HashMap;
import java.util.Set;

public class RowMap extends HashMap<String, Object> {
	private final HashMap<String, Object> data;

	public RowMap() {
		this.data = new HashMap<String, Object>();
		this.put("data", this.data);
	}

	public void setRowType(String type) {
		this.put("type", type);
	}

	public void putData(String key, Object value) {
		this.data.put(key,  value);
	}

	public void setTable(String name) {
		this.put("table", name);
	}

	public void setDatabase(String name) {
		this.put("database", name);
	}

	public void setTimestamp(Long l) {
		this.put("ts", l);
	}

	public void setXid(Long xid) {
		this.put("xid", xid);
	}

	public void setTXCommit() {
		this.put("commit", true);
	}

	public Object getData(String string) {
		return this.data.get(string);
	}

	public Set<String> dataKeySet() {
		return this.data.keySet();
	}
}
