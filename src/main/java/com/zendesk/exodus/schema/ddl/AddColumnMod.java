package com.zendesk.exodus.schema.ddl;

import com.zendesk.exodus.schema.Table;
import com.zendesk.exodus.schema.columndef.ColumnDef;

class AddColumnMod extends ColumnMod {
	public ColumnDef definition;
	public ColumnPosition position;

	public AddColumnMod(String name, ColumnDef d, ColumnPosition position) {
		super(name);
		this.definition = d;
		this.position = position;
	}

	@Override
	public void apply(Table table) throws SchemaSyncError {
		table.addColumn(position.index(table, null), this.definition);
	}
}

