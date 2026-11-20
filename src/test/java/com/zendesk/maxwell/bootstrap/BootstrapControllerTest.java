package com.zendesk.maxwell.bootstrap;

import com.zendesk.maxwell.producer.AbstractProducer;
import com.zendesk.maxwell.util.ConnectionPool;
import org.junit.Test;

import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BootstrapControllerTest {
	private BootstrapController failingController() throws SQLException {
		ConnectionPool pool = mock(ConnectionPool.class);
		when(pool.getConnection()).thenThrow(new SQLException("boom"));

		return new BootstrapController(
			pool,
			mock(AbstractProducer.class),
			mock(SynchronousBootstrapper.class),
			"maxwell",
			false,
			0L
		);
	}

	@Test
	public void testBacksOffAfterSQLException() throws Exception {
		BootstrapController controller = failingController();

		long start = System.currentTimeMillis();
		controller.work();
		long elapsed = System.currentTimeMillis() - start;

		// a failing pass must wait before the run loop tries again, same as a successful one
		assertThat(elapsed, greaterThanOrEqualTo(1000L));
	}

	@Test
	public void testInterruptCutsBackoffShort() throws Exception {
		BootstrapController controller = failingController();
		AtomicReference<Exception> error = new AtomicReference<>();

		Thread worker = new Thread(() -> {
			try {
				controller.work();
			} catch ( Exception e ) {
				error.set(e);
			}
		});

		long start = System.currentTimeMillis();
		worker.start();
		Thread.sleep(100);

		// what runBootstrapNow() and requestStop() do
		worker.interrupt();
		worker.join(5000);
		long elapsed = System.currentTimeMillis() - start;

		assertThat(error.get(), nullValue());
		assertThat(elapsed, lessThan(1000L));
	}
}
