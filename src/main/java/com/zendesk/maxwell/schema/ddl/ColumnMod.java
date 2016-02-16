package com.zendesk.maxwell.schema.ddl;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.zendesk.maxwell.schema.Table;

@JsonTypeInfo(use=JsonTypeInfo.Id.NAME, include=JsonTypeInfo.As.PROPERTY, property="type")
@JsonSubTypes({
		@JsonSubTypes.Type(value = AddColumnMod.class, name = "column-add"),
		@JsonSubTypes.Type(value = ChangeColumnMod.class, name = "column-modify"),
		@JsonSubTypes.Type(value = RemoveColumnMod.class, name = "column-drop")
})
abstract class ColumnMod {
	public String name;

	public ColumnMod() {}
	public ColumnMod(String name) {
		this.name = name;
	}

	protected int originalIndex(Table table) throws SchemaSyncError {
		int originalIndex = table.findColumnIndex(name);

		if ( originalIndex == -1 )
			throw new SchemaSyncError("Could not find column " + name + " in " + table.getName());

		return originalIndex;
	}

	public abstract void apply(Table table) throws SchemaSyncError;
}
