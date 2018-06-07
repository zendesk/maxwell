package com.zendesk.maxwell.api.schema;

public class InvalidSchemaError extends Exception {
	public InvalidSchemaError (String message) { super(message); }
	private static final long serialVersionUID = 1L;
}
