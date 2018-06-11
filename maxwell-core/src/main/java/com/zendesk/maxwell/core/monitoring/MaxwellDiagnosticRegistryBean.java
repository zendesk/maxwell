package com.zendesk.maxwell.core.monitoring;

import com.zendesk.maxwell.api.MaxwellTerminationListener;
import com.zendesk.maxwell.api.monitoring.MaxwellDiagnostic;
import com.zendesk.maxwell.api.monitoring.MaxwellDiagnosticRegistry;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class MaxwellDiagnosticRegistryBean implements MaxwellDiagnosticRegistry, MaxwellTerminationListener {
	public final List<MaxwellDiagnostic> diagnostics = new ArrayList<>();

	@Override
	public void registerDiagnostic(MaxwellDiagnostic diagnostic){
		if(diagnostic != null){
			this.diagnostics.add(diagnostic);
		}
	}

	@Override
	public List<MaxwellDiagnostic> getDiagnostics() {
		return diagnostics;
	}

	@Override
	public void onTermination() {
		diagnostics.clear();
	}
}
