package com.zendesk.maxwell.schema.columndef;

public class FloatColumnDef extends ColumnDef {
	private FloatColumnDef(String name, String type, short pos) {
		super(name, type, pos);
	}

	public static FloatColumnDef create(String name, String type, short pos) {
		FloatColumnDef temp = new FloatColumnDef(name, type, pos);
		return (FloatColumnDef) INTERNER.intern(temp);
	}

	@Override
	public String toSQL(Object value) {
		return value.toString();
	}
}
