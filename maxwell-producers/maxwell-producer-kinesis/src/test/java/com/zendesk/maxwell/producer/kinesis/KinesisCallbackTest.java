package com.zendesk.maxwell.producer.kinesis;

import com.amazonaws.services.kinesis.producer.IrrecoverableError;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.zendesk.maxwell.core.MaxwellContext;
import com.zendesk.maxwell.core.config.MaxwellConfig;
import com.zendesk.maxwell.core.replication.BinlogPosition;
import com.zendesk.maxwell.core.replication.Position;
import com.zendesk.maxwell.core.producer.AbstractAsyncProducer;
import org.junit.Test;

import static org.mockito.Mockito.*;

public class KinesisCallbackTest {

	@Test
	public void shouldIgnoreProducerErrorByDefault() {
		final MaxwellContext context = mock(MaxwellContext.class);
		final MaxwellConfig config = mock(MaxwellConfig.class);
		when(context.getConfig()).thenReturn(config);
		when(config.isIgnoreProducerError()).thenReturn(true);

		final AbstractAsyncProducer.CallbackCompleter cc = mock(AbstractAsyncProducer.CallbackCompleter.class);
		final BinlogPosition binlogPosition = new BinlogPosition(1, "binlog-1");
		final Position position = new Position(binlogPosition, 0L);

		KinesisCallback callback = new KinesisCallback(cc, position, "key", "value", new Counter(), new Counter(), new Meter(), new Meter(), context);
		IrrecoverableError error = new IrrecoverableError("blah");
		callback.onFailure(error);

		verify(cc).markCompleted();
	}

	@Test
	public void shouldTerminateWhenNotIgnoreProducerError() {
		final MaxwellContext context = mock(MaxwellContext.class);
		final MaxwellConfig config = mock(MaxwellConfig.class);
		when(config.isIgnoreProducerError()).thenReturn(false);
		when(context.getConfig()).thenReturn(config);

		final AbstractAsyncProducer.CallbackCompleter cc = mock(AbstractAsyncProducer.CallbackCompleter.class);
		final BinlogPosition binlogPosition = new BinlogPosition(1, "binlog-1");
		final Position position = new Position(binlogPosition, 0L);

		KinesisCallback callback = new KinesisCallback(cc, position, "key", "value", new Counter(), new Counter(), new Meter(), new Meter(), context);
		IrrecoverableError error = new IrrecoverableError("blah");
		callback.onFailure(error);

		verify(context).terminate(any(RuntimeException.class));
		verifyZeroInteractions(cc);
	}
}
