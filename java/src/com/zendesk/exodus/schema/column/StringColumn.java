package com.zendesk.exodus.schema.column;

public class StringColumn extends AbstractColumn {
	private final String encoding;
	public StringColumn(String tableName, String name, String type, int pos, String encoding) {
		super(tableName, name, type, pos);
		this.encoding = encoding;
	}
}
