package com.zendesk.exodus.schema.column;

public class BigIntColumn extends Column {
	private final boolean signed;

	public BigIntColumn(String tableName, String name, String type, int pos, boolean signed) {
		super(tableName, name, type, pos);
		this.signed = signed;
	}

}
