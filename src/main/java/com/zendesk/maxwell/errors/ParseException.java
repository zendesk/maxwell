package com.zendesk.maxwell.errors;

public class ParseException extends RuntimeException {
	public ParseException (String message) {
		super(message);
	}
}
