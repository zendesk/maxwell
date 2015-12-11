package com.zendesk.maxwell.schema.ddl;

import com.zendesk.maxwell.schema.Database;
import com.zendesk.maxwell.schema.Schema;

public class DatabaseAlter extends SchemaChange {
	private final String dbName;
	public String characterSet;

	public DatabaseAlter(String dbName) {
		this.dbName = dbName;
	}

	@Override
	public Schema apply(Schema originalSchema) throws SchemaSyncError {
		if ( characterSet == null )
			return originalSchema;

		Schema schema = originalSchema.copy();
		Database d = schema.findDatabase(dbName);

		if ( d == null )
			throw new SchemaSyncError("Couldn't find database: " + dbName + " while applying database alter");

		if ( d.getEncoding().equals(characterSet) )
			return originalSchema;

		d.setEncoding(characterSet);
		return schema;
	}
}
