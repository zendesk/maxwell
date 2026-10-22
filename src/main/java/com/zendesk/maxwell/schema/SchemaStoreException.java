package com.zendesk.maxwell.schema;

import java.io.Serial;

public class SchemaStoreException extends Exception {
	public SchemaStoreException (String message) { super(message); }
	public SchemaStoreException (Exception e) { super(e); }
	@Serial private static final long serialVersionUID = 1L;
}
