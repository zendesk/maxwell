package com.zendesk.maxwell.schema.ddl;

import org.antlr.v4.runtime.misc.ParseCancellationException;

public class ReparseSQLException extends ParseCancellationException {
	private final String sql;

	public ReparseSQLException(String sql) {
		this.sql = sql;
	}

	public String getSQL() {
		return this.sql;
	}
}
