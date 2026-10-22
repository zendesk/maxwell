package com.zendesk.maxwell.errors;

import java.io.Serial;

public class DuplicateProcessException extends Exception {
        public DuplicateProcessException (String message) { super(message); }
	@Serial private static final long serialVersionUID = 1L;
}
