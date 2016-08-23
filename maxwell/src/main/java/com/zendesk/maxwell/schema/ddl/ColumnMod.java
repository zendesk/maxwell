package com.zendesk.maxwell.schema.ddl;

import com.zendesk.maxwell.schema.Table;

abstract class ColumnMod {
	public String name;

	public ColumnMod(String name) {
		this.name = name;
	}

	protected int originalIndex(Table table) throws InvalidSchemaError {
		int originalIndex = table.findColumnIndex(name);

		if ( originalIndex == -1 )
			throw new InvalidSchemaError("Could not find column " + name + " in " + table.getName());

		return originalIndex;
	}

	public abstract void apply(Table table) throws InvalidSchemaError;
}
