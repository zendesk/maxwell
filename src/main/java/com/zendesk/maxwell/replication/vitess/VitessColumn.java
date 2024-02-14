package com.zendesk.maxwell.replication.vitess;

public class VitessColumn {
	private final String name;
	private final VitessType type;

	public VitessColumn(String name, VitessType type) {
		this.name = name.intern();
		this.type = type;
	}

	public String getName() {
		return name;
	}

	public VitessType getType() {
		return type;
	}

	public String toString() {
		return "Column [name=" + name + ", type=" + type + "]";
	}
}
