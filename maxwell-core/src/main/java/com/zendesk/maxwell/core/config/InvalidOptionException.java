package com.zendesk.maxwell.core.config;

public class InvalidOptionException extends RuntimeException {
	private final String[] filterOptions;

	public InvalidOptionException(String message, String... filterOptions) {
		this(message, null, filterOptions);
	}

	public InvalidOptionException(String message, Throwable cause, String... filterOptions) {
		super(message);
		this.filterOptions = filterOptions;
	}

	public String[] getFilterOptions() {
		return filterOptions;
	}
}
