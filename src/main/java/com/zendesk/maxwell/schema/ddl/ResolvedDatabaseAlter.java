package com.zendesk.maxwell.schema.ddl;

import com.zendesk.maxwell.MaxwellFilter;
import com.zendesk.maxwell.schema.*;

public class ResolvedDatabaseAlter extends ResolvedSchemaChange {
	public String database;
	public String charset;

	public ResolvedDatabaseAlter() {}
	public ResolvedDatabaseAlter(String database, String charset) {
		this.database = database;
		this.charset = charset;
	}

	@Override
	public void apply(Schema schema) throws InvalidSchemaError {
		if ( charset == null )
			return;

		Database d = schema.findDatabaseOrThrow(database);

		if ( !d.getCharset().equals(charset) )
			d.setCharset(charset);
	}
}
