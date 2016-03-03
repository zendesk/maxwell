package com.zendesk.maxwell.schema.ddl;

public class SchemaSyncError extends Exception {
	public SchemaSyncError (String message) { super(message); }
	private static final long serialVersionUID = 1L;
}
