package com.zendesk.maxwell.monitoring;

import java.util.concurrent.CompletableFuture;

public interface MaxwellDiagnostic {

	String getName();

	boolean isMandatory();

	String getResource();

	CompletableFuture<MaxwellDiagnosticResult.Check> check();

}
