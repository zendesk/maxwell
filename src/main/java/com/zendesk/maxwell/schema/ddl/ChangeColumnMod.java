package com.zendesk.maxwell.schema.ddl;

import com.zendesk.maxwell.schema.Table;
import com.zendesk.maxwell.schema.columndef.ColumnDef;

class ChangeColumnMod extends ColumnMod {
	public ColumnDef definition;
	public ColumnPosition position;

	public ChangeColumnMod(String name, ColumnDef d, ColumnPosition position ) {
		super(name);
		this.definition = d;
		this.position = position;
	}

	@Override
	public void apply(Table table) throws InvalidSchemaError {
		int idx = originalIndex(table);
		table.changeColumn(idx, position, definition);
	}
}

