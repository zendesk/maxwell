package com.zendesk.maxwell.schema.ddl;

import com.zendesk.maxwell.filtering.FilterV2;
import com.zendesk.maxwell.schema.*;
import com.zendesk.maxwell.schema.ddl.ResolvedDatabaseCreate;

public class DatabaseCreate extends SchemaChange {
	public final String database;
	private final boolean ifNotExists;
	public final String charset;

	public DatabaseCreate(String database, boolean ifNotExists, String charset) {
		this.database = database;
		this.ifNotExists = ifNotExists;
		this.charset = charset;
	}

	@Override
	public ResolvedDatabaseCreate resolve(Schema schema) throws InvalidSchemaError {
		if ( ifNotExists && schema.hasDatabase(database) )
			return null;

		String chset;
		if ( this.charset == null )
			chset = schema.getCharset();
		else
			chset = this.charset;

		return new ResolvedDatabaseCreate(database, chset);
	}

	@Override
	public boolean isBlacklisted(FilterV2 filter) {
		if ( filter == null ) {
			return false;
		} else {
			return filter.isDatabaseBlacklisted(database);
		}
	}

}
