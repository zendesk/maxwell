package com.zendesk.maxwell.schema.columndef;

public class FloatColumnDef extends ColumnDef {
	public FloatColumnDef() { }
	public FloatColumnDef(String name, String type, short pos, boolean nullable) {
		super(name, type, pos, nullable);
	}

	public boolean signed;

	@Override
	public String toSQL(Object value) {
		return value.toString();
	}
}
