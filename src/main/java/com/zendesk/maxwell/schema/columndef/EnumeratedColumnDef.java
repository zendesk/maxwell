package com.zendesk.maxwell.schema.columndef;

import com.fasterxml.jackson.annotation.JsonProperty;

abstract public class EnumeratedColumnDef extends ColumnDef  {
	@JsonProperty("enum-values")
	protected String[] enumValues;

	public EnumeratedColumnDef(String name, String type, short pos, String [] enumValues, boolean nullable) {
		super(name, type, pos, nullable);
		this.enumValues = new String[enumValues.length];
		for ( int i = 0; i < enumValues.length; i++)
			this.enumValues[i] = enumValues[i].intern();
	}

	public String[] getEnumValues() {
		return enumValues;
	}
}
