package com.zendesk.maxwell.schema.ddl;

import com.zendesk.maxwell.MaxwellFilter;
import com.zendesk.maxwell.schema.Database;
import com.zendesk.maxwell.schema.Schema;
import com.zendesk.maxwell.schema.Table;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;


@JsonInclude(JsonInclude.Include.NON_NULL)
public class TableDrop extends SchemaChange {
	public String database;
	public String table;
	private boolean ifExists;

	public TableDrop() { }
	public TableDrop(String database, String table, boolean ifExists) {
		this.database = database;
		this.table = table;
		this.ifExists = ifExists;
	}

	@Override
	public TableDrop resolve(Schema schema) {
		if ( ifExists ) {
			Database d = schema.findDatabase(this.database);
			if ( d == null || !d.hasTable(table) )
				return null;
		}

		return new TableDrop(database, table, false);
	}

	@Override
	public Schema apply(Schema originalSchema) throws SchemaSyncError {
		Schema newSchema = originalSchema.copy();

		Database d = newSchema.findDatabase(this.database);
		if ( d == null || !d.hasTable(this.table) )
			throw new SchemaSyncError("Can't drop non-existant table: " + this.database + "." + this.table);

		d.removeTable(this.table);
		return newSchema;
	}

	@Override
	public boolean isBlacklisted(MaxwellFilter filter) {
		if ( filter == null ) {
			return false;
		} else {
			return filter.isTableBlacklisted(this.table);
		}
	}

}
