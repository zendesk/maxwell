package com.zendesk.maxwell.schema.ddl;

public class DeferredPositionUpdate {
	public String column;
	public ColumnPosition position;

	public DeferredPositionUpdate(String column, ColumnPosition position) {
		this.column = column;
		this.position = position;
	}
}
