package com.zendesk.exodus.schema.column;


public class IntColumn extends Column {
	private final boolean signed;

	public IntColumn(String tableName, String name, String type, int pos, boolean signed) {
		super(tableName, name, type, pos);
		this.signed = signed;
	}
}
