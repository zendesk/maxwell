package com.zendesk.maxwell;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.zendesk.maxwell.metrics.MaxwellMetrics;
import com.zendesk.maxwell.producer.BufferedProducer;
import com.zendesk.maxwell.row.RowMap;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * @author Geoff Lywood (geoff@addepar.com)
 */
public class EmbeddedMaxwellTest extends MaxwellTestWithIsolatedServer {

	@Test
	public void testCustomMetricsAndProducer() throws Exception {
		MaxwellConfig config = getConfig(server);
		MetricRegistry metrics = new MetricRegistry();
		HealthCheckRegistry healthChecks = new HealthCheckRegistry();
		config.metricRegistry = metrics;
		config.healthCheckRegistry = healthChecks;
		config.metricsPrefix = "prefix";

		BufferedProducer bufferedProducer = new BufferedProducer(1);
		config.producer = bufferedProducer;

		final CountDownLatch latch = new CountDownLatch(1);
		Maxwell maxwell = new Maxwell(config) {
			@Override
			protected void onReplicatorStart() {
				latch.countDown();
			}
		};
		new Thread(maxwell).start();
		latch.await();

		server.execute("insert into minimal set account_id = 1, text_field='hello'");
		RowMap rowMap = bufferedProducer.poll(10, TimeUnit.SECONDS);

		maxwell.terminate();
		Exception maxwellError = maxwell.context.getError();
		if (maxwellError != null) {
			throw maxwellError;
		}

		assertThat(rowMap, is(notNullValue()));
		assertTrue(metrics.getCounters().get("prefix.row.count").getCount() > 0);
	}

	private MaxwellConfig getConfig(MysqlIsolatedServer mysql) {
		MaxwellConfig config = new MaxwellConfig();
		config.maxwellMysql.user = "maxwell";
		config.maxwellMysql.password = "maxwell";
		config.maxwellMysql.host = "localhost";
		config.maxwellMysql.port = mysql.getPort();
		config.maxwellMysql.jdbcOptions.add("useSSL=false");
		config.replicationMysql = config.maxwellMysql;
		return config;
	}
}
