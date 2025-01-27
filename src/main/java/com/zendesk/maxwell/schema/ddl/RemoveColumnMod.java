package com.zendesk.maxwell.schema.ddl;

import com.zendesk.maxwell.schema.Table;

import java.util.List;

class RemoveColumnMod extends ColumnMod {
	private final boolean ifExists;
	public RemoveColumnMod(String name, boolean ifExists) {
		super(name);
		this.ifExists = ifExists;
	}

	@Override
	public void apply(Table table, List<DeferredPositionUpdate> deferred) throws InvalidSchemaError {
		int originalIndex = table.findColumnIndex(name);

		if ( originalIndex == -1 ) {
			if ( ifExists )
				return;
			else
				throw new InvalidSchemaError("Could not find column " + name + " in " + table.getName());
		}

		table.removeColumn(originalIndex);
	}
}
