package com.zendesk.exodus.schema.ddl;

import com.zendesk.exodus.schema.Table;
import com.zendesk.exodus.schema.columndef.ColumnDef;

class ChangeColumnMod extends ColumnMod {
	public ColumnDef definition;
	public ColumnPosition position;

	public ChangeColumnMod(String name, ColumnDef d, ColumnPosition position ) {
		super(name);
		this.definition = d;
		this.position = position;
	}

	@Override
	public void apply(Table table) throws SchemaSyncError {
		int idx = originalIndex(table);
		table.removeColumn(idx);
		table.addColumn(position.index(table, idx), this.definition);
	}
}

