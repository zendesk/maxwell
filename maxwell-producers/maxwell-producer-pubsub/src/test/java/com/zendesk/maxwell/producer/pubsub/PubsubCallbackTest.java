package com.zendesk.maxwell.producer.pubsub;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.zendesk.maxwell.core.MaxwellContext;
import com.zendesk.maxwell.api.config.MaxwellConfig;
import com.zendesk.maxwell.api.replication.BinlogPosition;
import com.zendesk.maxwell.api.replication.Position;
import com.zendesk.maxwell.core.producer.AbstractAsyncProducer;
import org.junit.Test;

import static org.mockito.Mockito.*;

public class PubsubCallbackTest {

	@Test
	public void shouldIgnoreProducerErrorByDefault() {
		final MaxwellContext context = mock(MaxwellContext.class);
		final MaxwellConfig config = mock(MaxwellConfig.class);
		when(context.getConfig()).thenReturn(config);
		when(config.isIgnoreProducerError()).thenReturn(true);
		when(context.getConfig()).thenReturn(config);

		final AbstractAsyncProducer.CallbackCompleter cc = mock(AbstractAsyncProducer.CallbackCompleter.class);
		final BinlogPosition binlogPosition = new BinlogPosition(1, "binlog-1");
		final Position position = new Position(binlogPosition, 0L);

		PubsubCallback callback = new PubsubCallback(cc, position, "value", new Counter(), new Counter(), new Meter(), new Meter(), context);
		Throwable t = new Throwable("blah");
		callback.onFailure(t);

		verify(cc).markCompleted();
	}

	@Test
	public void shouldTerminateWhenNotIgnoreProducerError() {
		final MaxwellContext context = mock(MaxwellContext.class);
		final MaxwellConfig config = mock(MaxwellConfig.class);
		when(context.getConfig()).thenReturn(config);
		when(config.isIgnoreProducerError()).thenReturn(false);
		when(context.getConfig()).thenReturn(config);

		final AbstractAsyncProducer.CallbackCompleter cc = mock(AbstractAsyncProducer.CallbackCompleter.class);
		final BinlogPosition binlogPosition = new BinlogPosition(1, "binlog-1");
		final Position position = new Position(binlogPosition, 0L);

		PubsubCallback callback = new PubsubCallback(cc, position, "value", new Counter(), new Counter(), new Meter(), new Meter(), context);
		Throwable t = new Throwable("blah");
		callback.onFailure(t);

		verify(context).terminate(any(RuntimeException.class));
		verifyZeroInteractions(cc);
	}
}
