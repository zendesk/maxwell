package com.zendesk.maxwell.schema;

public class SchemaStoreException extends Exception {
	public SchemaStoreException (String message) { super(message); }
	public SchemaStoreException (Exception e) { super(e); }
	private static final long serialVersionUID = 1L;
}
