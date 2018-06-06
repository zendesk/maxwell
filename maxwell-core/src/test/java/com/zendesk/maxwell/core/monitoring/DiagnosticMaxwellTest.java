package com.zendesk.maxwell.core.monitoring;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zendesk.maxwell.core.*;
import com.zendesk.maxwell.core.config.BaseMaxwellDiagnosticConfig;
import com.zendesk.maxwell.api.config.MaxwellDiagnosticConfig;
import com.zendesk.maxwell.core.replication.BinlogConnectorDiagnostic;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DiagnosticMaxwellTest extends MaxwellTestWithIsolatedServer {

	@Autowired
	private MaxwellRunner maxwellRunner;

	private ByteArrayOutputStream outputStream;
	private PrintWriter writer;

	@Before
	public void setup() {
		outputStream = new ByteArrayOutputStream();
		writer = new PrintWriter(outputStream);
	}

	@After
	public void cleanup() throws IOException {
		writer.close();
		outputStream.close();
	}

	@Test
	public void testNormalBinlogReplicationDiagnostic() throws Exception {
		// Given
		BaseMaxwellDiagnosticConfig config = new BaseMaxwellDiagnosticConfig();
		config.setTimeout(5000);
		MaxwellContext maxwellContext = buildContext();

		DiagnosticHealthCheck healthCheck = getDiagnosticHealthCheck(config, maxwellContext);

		HttpServletRequest request = mock(HttpServletRequest.class);
		HttpServletResponse response = mock(HttpServletResponse.class);
		when(response.getWriter()).thenReturn(writer);

		final CountDownLatch latch = new CountDownLatch(1);
		maxwellContext.configureOnReplicationStartEventHandler(context -> latch.countDown());
		new Thread(() -> maxwellRunner.run(maxwellContext)).start();
		latch.await();

		// When
		healthCheck.doGet(request, response);
		writer.flush();

		// Then
		JsonNode binlogNode = getBinlogNode();
		assertThat(binlogNode.get("name").asText(), is("binlog-connector"));
		assertThat(binlogNode.get("success").asBoolean(), is(true));
		assertTrue(binlogNode.get("mandatory").asBoolean());
		assertTrue(binlogNode.get("message").asText().contains("Binlog replication lag"));
		maxwellRunner.terminate(maxwellContext);
	}

	@Test
	public void testBinlogReplicationDiagnosticTimeout() throws Exception {
		// Given
		BaseMaxwellDiagnosticConfig config = new BaseMaxwellDiagnosticConfig();
		config.setTimeout(100);
		MaxwellContext maxwellContext = buildContext();

		DiagnosticHealthCheck healthCheck = getDiagnosticHealthCheck(config, maxwellContext);

		HttpServletRequest request = mock(HttpServletRequest.class);
		HttpServletResponse response = mock(HttpServletResponse.class);
		when(response.getWriter()).thenReturn(writer);

		// When
		healthCheck.doGet(request, response);
		writer.flush();

		// Then
		JsonNode binlogNode = getBinlogNode();
		assertThat(binlogNode.get("name").asText(), is("binlog-connector"));
		assertThat(binlogNode.get("success").asBoolean(), is(false));
		assertTrue(binlogNode.get("mandatory").asBoolean());
		assertTrue(binlogNode.get("message").asText().contains("check did not return after 100 ms"));
	}

	private DiagnosticHealthCheck getDiagnosticHealthCheck(MaxwellDiagnosticConfig config, MaxwellContext maxwellContext) throws ServletException {
		MaxwellDiagnosticContext diagnosticContext = new MaxwellDiagnosticContext(config,
				Collections.singletonList(new BinlogConnectorDiagnostic(maxwellContext)));
		DiagnosticHealthCheck healthCheck = new DiagnosticHealthCheck(diagnosticContext);
		healthCheck.init(healthCheck);
		return healthCheck;
	}

	private JsonNode getBinlogNode() throws IOException {
		ObjectMapper objectMapper = new ObjectMapper();
		JsonNode rootNode = objectMapper.readTree(outputStream.toByteArray());
		JsonNode checksNode = rootNode.path("checks");
		return checksNode.get(0);
	}
}
