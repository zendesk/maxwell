package com.zendesk.maxwell.schema.ddl;

import com.zendesk.maxwell.MaxwellFilter;
import com.zendesk.maxwell.schema.Database;
import com.zendesk.maxwell.schema.Schema;

public class DatabaseAlter extends SchemaChange {
	public String database;
	public String charset;

	public DatabaseAlter(String database) {
		this.database = database;
	}

	@Override
	public Schema apply(Schema originalSchema) throws SchemaSyncError {
		if ( charset == null )
			return originalSchema;

		Schema schema = originalSchema.copy();
		Database d = schema.findDatabase(database);

		if ( d == null )
			throw new SchemaSyncError("Couldn't find database: " + database + " while applying database alter");

		if ( d.getCharset().equals(charset) )
			return originalSchema;

		d.setCharset(charset);
		return schema;
	}

	@Override
	public boolean isBlacklisted(MaxwellFilter filter) {
		if ( filter == null ) {
			return false;
		} else {
			return filter.isDatabaseBlacklisted(database);
		}
	}
}
