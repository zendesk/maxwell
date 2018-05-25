package com.zendesk.maxwell.core.producer;

import com.amazonaws.services.kinesis.producer.IrrecoverableError;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.zendesk.maxwell.core.MaxwellContext;
import com.zendesk.maxwell.core.config.MaxwellConfig;
import com.zendesk.maxwell.core.config.MaxwellConfigFactory;
import com.zendesk.maxwell.core.replication.BinlogPosition;
import com.zendesk.maxwell.core.replication.Position;
import org.junit.Test;

import static org.mockito.Mockito.*;

public class KinesisCallbackTest {

	@Test
	public void shouldIgnoreProducerErrorByDefault() {
		MaxwellContext context = mock(MaxwellContext.class);
		MaxwellConfig config = new MaxwellConfigFactory().createNewDefaultConfiguration();
		when(context.getConfig()).thenReturn(config);
		AbstractAsyncProducer.CallbackCompleter cc = mock(AbstractAsyncProducer.CallbackCompleter.class);
		KinesisCallback callback = new KinesisCallback(cc,
			new Position(new BinlogPosition(1, "binlog-1"), 0L), "key", "value",
				new Counter(), new Counter(), new Meter(), new Meter(), context);
		IrrecoverableError error = new IrrecoverableError("blah");
		callback.onFailure(error);
		verify(cc).markCompleted();
	}

	@Test
	public void shouldTerminateWhenNotIgnoreProducerError() {
		MaxwellContext context = mock(MaxwellContext.class);
		MaxwellConfig config = new MaxwellConfigFactory().createNewDefaultConfiguration();
		config.ignoreProducerError = false;
		when(context.getConfig()).thenReturn(config);
		AbstractAsyncProducer.CallbackCompleter cc = mock(AbstractAsyncProducer.CallbackCompleter.class);
		KinesisCallback callback = new KinesisCallback(cc,
			new Position(new BinlogPosition(1, "binlog-1"), 0L), "key", "value",
				new Counter(), new Counter(), new Meter(), new Meter(), context);
		IrrecoverableError error = new IrrecoverableError("blah");
		callback.onFailure(error);
		verify(context).terminate(any(RuntimeException.class));
		verifyZeroInteractions(cc);
	}
}
