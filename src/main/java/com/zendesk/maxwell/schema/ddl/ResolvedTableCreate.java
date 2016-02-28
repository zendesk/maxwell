package com.zendesk.maxwell.schema.ddl;

import com.fasterxml.jackson.annotation.JsonUnwrapped;
import com.zendesk.maxwell.schema.*;
import com.zendesk.maxwell.MaxwellFilter;

public class ResolvedTableCreate extends ResolvedSchemaChange {
    @JsonUnwrapped
	public Table table;

	public ResolvedTableCreate() {}
	public ResolvedTableCreate(Table t) {
		this.table = t;
	}

	@Override
	public Schema apply(Schema originalSchema) throws SchemaSyncError {
		Schema newSchema = originalSchema.copy();

		Database d = newSchema.findDatabaseOrThrow(this.table.database);

		if ( d.hasTable(this.table.name) )
			throw new SchemaSyncError("Unexpectedly asked to create existing table " + this.table.name);

		d.addTable(this.table);
		return newSchema;
	}
}
