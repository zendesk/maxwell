package com.zendesk.maxwell.core.monitoring;

import com.zendesk.maxwell.core.config.MaxwellDiagnosticConfig;

import java.util.List;

public class MaxwellDiagnosticContext {

	public final MaxwellDiagnosticConfig config;
	public final List<MaxwellDiagnostic> diagnostics;

	public MaxwellDiagnosticContext(MaxwellDiagnosticConfig config, List<MaxwellDiagnostic> diagnostics) {
		this.config = config;
		this.diagnostics = diagnostics;
	}

}
