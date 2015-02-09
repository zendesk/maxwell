package com.zendesk.maxwell.schema.ddl;

import com.zendesk.maxwell.schema.Table;

class RemoveColumnMod extends ColumnMod {
	public RemoveColumnMod(String name) {
		super(name);
	}

	@Override
	public void apply(Table table) throws SchemaSyncError {
		table.removeColumn(originalIndex(table));
	}
}