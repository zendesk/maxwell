package com.zendesk.maxwell.metricreporter.http;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.shyiko.mysql.binlog.network.SSLMode;
import com.zendesk.maxwell.core.config.MaxwellConfig;
import com.zendesk.maxwell.core.config.MaxwellMysqlConfig;
import com.zendesk.maxwell.core.monitoring.MaxwellDiagnosticRegistry;
import com.zendesk.maxwell.core.MaxwellContextFactory;
import com.zendesk.maxwell.core.MaxwellRunner;
import com.zendesk.maxwell.core.MaxwellSystemContext;
import com.zendesk.maxwell.core.config.MaxwellConfigFactory;
import com.zendesk.maxwell.core.springconfig.CoreComponentScanConfig;
import com.zendesk.maxwell.core.util.Logging;
import com.zendesk.maxwell.metricreporter.core.springconfig.MetricsReporterCoreComponentScanConfig;
import com.zendesk.maxwell.metricreporter.http.springconfig.HttpMetricReporterComponentScanConfig;
import com.zendesk.maxwell.metricreporter.http.springconfig.MetricsTestConfig;
import com.zendesk.maxwell.core.util.test.mysql.MysqlIsolatedServer;
import com.zendesk.maxwell.core.util.test.mysql.MysqlIsolatedServerSupport;
import org.junit.*;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {MetricsTestConfig.class, CoreComponentScanConfig.class, MetricsReporterCoreComponentScanConfig.class, HttpMetricReporterComponentScanConfig.class })
public class DiagnosticMaxwellTest {

	@Autowired
	private MaxwellRunner maxwellRunner;
	@Autowired
	private MaxwellDiagnosticRegistry maxwellDiagnosticRegistry;
	@Autowired
	private MaxwellConfigFactory maxwellConfigFactory;
	@Autowired
	private MaxwellContextFactory maxwellContextFactory;
	@Autowired
	private MysqlIsolatedServerSupport mysqlIsolatedServerSupport;

	private ByteArrayOutputStream outputStream;
	private PrintWriter writer;

	private MysqlIsolatedServer server;

	@BeforeClass
	public static void setupTest() throws Exception {
		Logging.setupLogBridging();
	}

	@Before
	public void setup() throws Exception {
		server = mysqlIsolatedServerSupport.setupServer();
		mysqlIsolatedServerSupport.setupSchema(server);

		outputStream = new ByteArrayOutputStream();
		writer = new PrintWriter(outputStream);
	}

	@After
	public void cleanup() throws IOException {
		writer.close();
		outputStream.close();
		server.shutDown();
	}

	@Test
	public void testNormalBinlogReplicationDiagnostic() throws Exception {
		// Given
		HttpMetricReporterConfiguration configuration = new HttpMetricReporterConfiguration();
		configuration.setDiagnoticTimeout(5000);

		//Start context to register diagnostics
		MaxwellSystemContext maxwellContext = buildContext();
		DiagnosticHealthCheck healthCheck = getDiagnosticHealthCheck(configuration);

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
		HttpMetricReporterConfiguration configuration = new HttpMetricReporterConfiguration();
		configuration.setDiagnoticTimeout(100);

		//Start context to register diagnostics
		buildContext();

		DiagnosticHealthCheck healthCheck = getDiagnosticHealthCheck(configuration);

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

	private DiagnosticHealthCheck getDiagnosticHealthCheck(HttpMetricReporterConfiguration configuration) throws ServletException {
		DiagnosticHealthCheck healthCheck = new DiagnosticHealthCheck(configuration, maxwellDiagnosticRegistry);
		healthCheck.init(healthCheck);
		return healthCheck;
	}

	private JsonNode getBinlogNode() throws IOException {
		ObjectMapper objectMapper = new ObjectMapper();
		JsonNode rootNode = objectMapper.readTree(outputStream.toByteArray());
		JsonNode checksNode = rootNode.path("checks");
		return checksNode.get(0);
	}

	private MaxwellSystemContext buildContext() throws SQLException, URISyntaxException {
		return maxwellContextFactory.createFor(buildConfiguration());
	}

	private MaxwellConfig buildConfiguration(){
		Properties properties = new Properties();
		properties.put(MaxwellMysqlConfig.CONFIGURATION_OPTION_HOST, "127.0.0.1");
		properties.put(MaxwellMysqlConfig.CONFIGURATION_OPTION_PORT, "" + server.getPort());
		properties.put(MaxwellMysqlConfig.CONFIGURATION_OPTION_USER, "maxwell");
		properties.put(MaxwellMysqlConfig.CONFIGURATION_OPTION_PASSWORD, "maxwell");
		properties.put(MaxwellMysqlConfig.CONFIGURATION_OPTION_SSL, SSLMode.DISABLED.name());
		properties.put(MaxwellConfig.CONFIGURATION_OPTION_SCHEMA_DATABASE, "maxwell");
		properties.put(MaxwellConfig.CONFIGURATION_OPTION_PRODUCER, "buffer");
		return maxwellConfigFactory.createFor(properties);
	}
}
