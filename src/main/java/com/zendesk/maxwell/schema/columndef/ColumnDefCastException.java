package com.zendesk.maxwell.schema.columndef;

public class ColumnDefCastException extends Exception {
	public final Object givenValue;
	public final ColumnDef def;

	public String database;
	public String table;

	public ColumnDefCastException(ColumnDef def, Object givenValue) {
		super();
		this.def = def;
		this.givenValue = givenValue;
	}
}
