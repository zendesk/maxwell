package com.zendesk.maxwell.core.monitoring;

import com.zendesk.maxwell.core.MaxwellTerminationListener;
import com.zendesk.maxwell.api.monitoring.MaxwellDiagnostic;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class MaxwellDiagnosticRegistry implements MaxwellTerminationListener {
	public final List<MaxwellDiagnostic> diagnostics = new ArrayList<>();

	public void registerDiagnostic(MaxwellDiagnostic diagnostic){
		if(diagnostic != null){
			this.diagnostics.add(diagnostic);
		}
	}

	public List<MaxwellDiagnostic> getDiagnostics() {
		return diagnostics;
	}

	@Override
	public void onTermination() {
		diagnostics.clear();
	}
}
