package com.zendesk.maxwell.api.monitoring;

import java.util.List;

public interface MaxwellDiagnosticRegistry {
	void registerDiagnostic(MaxwellDiagnostic diagnostic);
	List<MaxwellDiagnostic> getDiagnostics();
}
