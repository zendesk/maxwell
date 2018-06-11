package com.zendesk.maxwell.core.schema.ddl;

import com.zendesk.maxwell.api.schema.InvalidSchemaError;
import com.zendesk.maxwell.core.schema.Table;
import com.zendesk.maxwell.core.schema.columndef.ColumnDef;

class AddColumnMod extends ColumnMod {
	public ColumnDef definition;
	public ColumnPosition position;

	public AddColumnMod(String name, ColumnDef d, ColumnPosition position) {
		super(name);
		this.definition = d;
		this.position = position;
	}

	@Override
	public void apply(Table table) throws InvalidSchemaError {
		table.addColumn(position.index(table, null), this.definition);
	}
}

