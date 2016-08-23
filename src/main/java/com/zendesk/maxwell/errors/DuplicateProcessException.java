package com.zendesk.maxwell.errors;

public class DuplicateProcessException extends Exception {
        public DuplicateProcessException (String message) { super(message); }
        private static final long serialVersionUID = 1L;
}
