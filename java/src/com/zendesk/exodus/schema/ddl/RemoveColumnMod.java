package com.zendesk.exodus.schema.ddl;

import com.zendesk.exodus.schema.Table;

class RemoveColumnMod extends ColumnMod {
	public RemoveColumnMod(String name) {
		super(name);
	}

	@Override
	public void apply(Table table) throws SchemaSyncError {
		table.removeColumn(originalIndex(table));
	}
}