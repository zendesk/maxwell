package com.zendesk.maxwell.schema.ddl;

import com.zendesk.maxwell.schema.Table;

import java.util.List;

public class RenameColumnMod extends ColumnMod  {
	private final String oldName, newName;

	public RenameColumnMod(String oldName, String newName) {
		super(oldName);
		this.oldName = oldName;
		this.newName = newName;
	}

	@Override
	public void apply(Table table, List<DeferredPositionUpdate> deferred) throws InvalidSchemaError {
		int idx = originalIndex(table);
		table.renameColumn(idx, newName);
	}
}
