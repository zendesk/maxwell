package com.zendesk.maxwell.core.schema.ddl;

import com.zendesk.maxwell.core.schema.InvalidSchemaError;
import com.zendesk.maxwell.core.schema.Table;

class RemoveColumnMod extends ColumnMod {
	public RemoveColumnMod(String name) {
		super(name);
	}

	@Override
	public void apply(Table table) throws InvalidSchemaError {
		table.removeColumn(originalIndex(table));
	}
}
