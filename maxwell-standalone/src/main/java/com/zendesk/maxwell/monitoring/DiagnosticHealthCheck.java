package com.zendesk.maxwell.monitoring;

import com.fasterxml.jackson.databind.ObjectMapper;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

public class DiagnosticHealthCheck extends HttpServlet {

	private static final String CONTENT_TYPE = "text/json";
	private static final String CACHE_CONTROL = "Cache-Control";
	private static final String NO_CACHE = "must-revalidate,no-cache,no-store";
	private final MaxwellDiagnosticContext diagnosticContext;
	protected transient ObjectMapper mapper;

	public DiagnosticHealthCheck(MaxwellDiagnosticContext diagnosticContext) {
		this.diagnosticContext = diagnosticContext;
	}

	@Override
	public void init(ServletConfig config) throws ServletException {
		super.init(config);

		this.mapper = new ObjectMapper().registerModule(new DiagnosticHealthCheckModule());
	}

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		resp.setHeader(CACHE_CONTROL, NO_CACHE);
		resp.setContentType(CONTENT_TYPE);
		Map<MaxwellDiagnostic, CompletableFuture<MaxwellDiagnosticResult.Check>> futureChecks = diagnosticContext.diagnostics.stream()
				.collect(Collectors.toMap(diagnostic -> diagnostic, MaxwellDiagnostic::check));

		List<MaxwellDiagnosticResult.Check> checks = futureChecks.entrySet().stream().map(future -> {
			CompletableFuture<MaxwellDiagnosticResult.Check> futureCheck = future.getValue();
			try {
				return futureCheck.get(diagnosticContext.config.timeout, TimeUnit.MILLISECONDS);
			} catch (InterruptedException | ExecutionException | TimeoutException e) {
				futureCheck.cancel(true);
				MaxwellDiagnostic diagnostic = future.getKey();
				Map<String, String> info = new HashMap<>();
				info.put("message", "check did not return after " + diagnosticContext.config.timeout + " ms");
				return new MaxwellDiagnosticResult.Check(diagnostic, false, info);
			}
		}).collect(Collectors.toList());

		MaxwellDiagnosticResult result = new MaxwellDiagnosticResult(checks);

		if (result.isSuccess()) {
			resp.setStatus(HttpServletResponse.SC_OK);
		} else if (result.isMandatoryFailed()) {
			resp.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
		} else {
			resp.setStatus(299);
		}

		try (PrintWriter output = resp.getWriter()) {
			mapper.writer().writeValue(output, result);
		}
	}
}
