package com.zendesk.maxwell.schema.ddl;

import java.io.Serial;

public class InvalidSchemaError extends Exception {
	public InvalidSchemaError (String message) { super(message); }
	@Serial private static final long serialVersionUID = 1L;
}
