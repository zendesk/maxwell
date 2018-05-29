package com.zendesk.maxwell.core.producer.impl.pubsub;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.zendesk.maxwell.core.MaxwellContext;
import com.zendesk.maxwell.core.SpringTestContextConfiguration;
import com.zendesk.maxwell.core.config.MaxwellConfig;
import com.zendesk.maxwell.core.config.MaxwellConfigFactory;
import com.zendesk.maxwell.core.producer.AbstractAsyncProducer;
import com.zendesk.maxwell.core.replication.BinlogPosition;
import com.zendesk.maxwell.core.replication.Position;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static org.mockito.Mockito.*;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {SpringTestContextConfiguration.class})
public class PubsubCallbackTest {

	@Autowired
	private MaxwellConfigFactory maxwellConfigFactory;

	@Test
	public void shouldIgnoreProducerErrorByDefault() {
		MaxwellContext context = mock(MaxwellContext.class);
		MaxwellConfig config = maxwellConfigFactory.createNewDefaultConfiguration();
		when(context.getConfig()).thenReturn(config);
		AbstractAsyncProducer.CallbackCompleter cc = mock(AbstractAsyncProducer.CallbackCompleter.class);
		PubsubCallback callback = new PubsubCallback(cc,
				new Position(new BinlogPosition(1, "binlog-1"), 0L), "value",
				new Counter(), new Counter(), new Meter(), new Meter(), context);
		Throwable t = new Throwable("blah");
		callback.onFailure(t);
		verify(cc).markCompleted();
	}

	@Test
	public void shouldTerminateWhenNotIgnoreProducerError() {
		MaxwellContext context = mock(MaxwellContext.class);
		MaxwellConfig config = maxwellConfigFactory.createNewDefaultConfiguration();
		config.setIgnoreProducerError(false);
		when(context.getConfig()).thenReturn(config);
		AbstractAsyncProducer.CallbackCompleter cc = mock(AbstractAsyncProducer.CallbackCompleter.class);
		PubsubCallback callback = new PubsubCallback(cc,
				new Position(new BinlogPosition(1, "binlog-1"), 0L), "value",
				new Counter(), new Counter(), new Meter(), new Meter(), context);
		Throwable t = new Throwable("blah");
		callback.onFailure(t);
		verify(context).terminate(any(RuntimeException.class));
		verifyZeroInteractions(cc);
	}
}
