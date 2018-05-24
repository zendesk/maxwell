package com.zendesk.maxwell.schema.ddl;

public class InvalidSchemaError extends Exception {
	public InvalidSchemaError (String message) { super(message); }
	private static final long serialVersionUID = 1L;
}
