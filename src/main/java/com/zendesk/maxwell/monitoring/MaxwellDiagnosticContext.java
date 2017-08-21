package com.zendesk.maxwell.monitoring;

import java.util.List;

public class MaxwellDiagnosticContext {

	public final Config config;
	public final List<MaxwellDiagnostic> diagnostics;

	public MaxwellDiagnosticContext(Config config, List<MaxwellDiagnostic> diagnostics) {
		this.config = config;
		this.diagnostics = diagnostics;
	}

	public static class Config {
		public boolean enable;
		public long timeout;
	}
}
