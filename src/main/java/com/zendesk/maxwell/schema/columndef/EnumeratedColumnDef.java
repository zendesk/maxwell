package com.zendesk.maxwell.schema.columndef;

abstract public class EnumeratedColumnDef extends ColumnDef  {
	protected String[] enumValues;

	public EnumeratedColumnDef(String name, String type, int pos, String [] enumValues) {
		super(name, type, pos);
		this.enumValues = enumValues;
	}

	public String[] getEnumValues() {
		return enumValues;
	}
}
