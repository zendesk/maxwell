package com.zendesk.maxwell;

/**
 * Thrown when Maxwell can't operate with the mysql server configured as it is.
 */
public class MaxwellCompatibilityError extends Exception {
	public MaxwellCompatibilityError(String message) {
		super(message);
	}
	private static final long serialVersionUID = 1L;
}
