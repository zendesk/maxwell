package com.zendesk.maxwell.schema.ddl;

import com.zendesk.maxwell.schema.Table;
import com.zendesk.maxwell.schema.columndef.ColumnDef;

import java.util.List;

class AddColumnMod extends ColumnMod {
	public ColumnDef definition;
	public ColumnPosition position;

	public AddColumnMod(String name, ColumnDef d, ColumnPosition position) {
		super(name);
		this.definition = d;
		this.position = position;
	}

	@Override
	public void apply(Table table, List<DeferredPositionUpdate> deferred) throws InvalidSchemaError {
		int index = position.index(table, null);

		if ( index == ColumnPosition.AFTER_NOT_FOUND) {
			deferred.add(new DeferredPositionUpdate(definition.getName(), position));
			index = 0;
		}

		table.addColumn(index, this.definition);

	}
}

