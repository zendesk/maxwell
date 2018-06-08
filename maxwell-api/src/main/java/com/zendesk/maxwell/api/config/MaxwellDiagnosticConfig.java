package com.zendesk.maxwell.api.config;

public interface MaxwellDiagnosticConfig {

	boolean DEFAULT_DIAGNOSTIC_HTTP = false;
	long DEFAULT_DIAGNOSTIC_HTTP_TIMEOUT = 10000L;

	boolean isEnable();

	long getTimeout();
}
