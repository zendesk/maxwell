package com.zendesk.maxwell.core;

import com.github.shyiko.mysql.binlog.network.SSLMode;
import com.zendesk.maxwell.core.producer.Producer;
import com.zendesk.maxwell.core.producer.ProducerFactory;
import com.zendesk.maxwell.core.row.RowMap;
import com.zendesk.maxwell.core.config.MaxwellConfig;
import com.zendesk.maxwell.core.config.MaxwellMysqlConfig;
import com.zendesk.maxwell.core.producer.AbstractProducer;
import com.zendesk.maxwell.core.config.MaxwellConfigFactory;
import com.zendesk.maxwell.core.util.test.mysql.MysqlIsolatedServer;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * @author Geoff Lywood (geoff@addepar.com)
 */
public class EmbeddedMaxwellTest extends MaxwellTestWithIsolatedServer {

	@Autowired
	private MaxwellConfigFactory maxwellConfigFactory;
	@Autowired
	private MaxwellContextFactory maxwellContextFactory;
	@Autowired
	private MaxwellRunner maxwell;

	@Test
	public void testCustomMetricsAndProducer() throws Exception {
		MaxwellConfig config = getConfig(server);
		config.metricsReportingType = "embedded";
		config.metricsPrefix = "prefix";
		config.producerFactory = EmbeddedTestProducerFactory.class.getName();

		final CountDownLatch latch = new CountDownLatch(1);
		final MaxwellContext maxwellContext = maxwellContextFactory.createFor(config);
		maxwellContext.configureOnReplicationStartEventHandler((context) -> latch.countDown());
		new Thread(() -> maxwell.run(maxwellContext)).start();
		latch.await();

		server.execute("insert into minimal set account_id = 1, text_field='hello'");
		RowMap rowMap = ((EmbeddedTestProducer)maxwellContext.getProducer()).poll(10, TimeUnit.SECONDS);

		maxwell.terminate(maxwellContext);
		Exception maxwellError = maxwellContext.getError();
		if (maxwellError != null) {
			throw maxwellError;
		}

		assertThat(rowMap, is(notNullValue()));
		assertTrue(maxwellContext.getMetrics().getRegistry().getCounters().get("prefix.row.count").getCount() > 0);
	}

	private MaxwellConfig getConfig(MysqlIsolatedServer mysql) {
		MaxwellConfig config = maxwellConfigFactory.create();
		MaxwellMysqlConfig maxwellMysql = config.maxwellMysql;
		maxwellMysql.user = "maxwell";
		maxwellMysql.password = "maxwell";
		maxwellMysql.host = "localhost";
		maxwellMysql.port = mysql.getPort();
		maxwellMysql.sslMode = SSLMode.DISABLED;
		config.replicationMysql = maxwellMysql;
		return config;
	}

	public static class EmbeddedTestProducerFactory implements ProducerFactory {
		@Override
		public Producer createProducer(MaxwellContext context) {
			return new EmbeddedTestProducer(context);
		}
	}

	public static class EmbeddedTestProducer extends AbstractProducer {
		private final BlockingQueue<RowMap> rowBuffer = new LinkedBlockingQueue<>();

		EmbeddedTestProducer(MaxwellContext context) {
			super(context);
		}

		@Override
		public void push(RowMap r) throws Exception {
			rowBuffer.put(r);
		}

		public RowMap poll(long timeout, TimeUnit timeUnit) throws InterruptedException {
			return rowBuffer.poll(timeout, timeUnit);
		}
	}
}
