package com.zendesk.maxwell;

import java.io.Serial;

/**
 * Thrown when Maxwell can't operate with the mysql server configured as it is.
 */
public class MaxwellCompatibilityError extends Exception {
	public MaxwellCompatibilityError(String message) {
		super(message);
	}
	@Serial private static final long serialVersionUID = 1L;
}
