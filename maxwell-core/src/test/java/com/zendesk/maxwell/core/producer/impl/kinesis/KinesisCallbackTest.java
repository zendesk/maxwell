package com.zendesk.maxwell.core.producer.impl.kinesis;

import com.amazonaws.services.kinesis.producer.IrrecoverableError;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.zendesk.maxwell.core.MaxwellContext;
import com.zendesk.maxwell.core.config.*;
import com.zendesk.maxwell.core.producer.AbstractAsyncProducer;
import com.zendesk.maxwell.core.producer.Producer;
import com.zendesk.maxwell.core.producer.ProducerExtensionConfigurators;
import com.zendesk.maxwell.core.replication.BinlogPosition;
import com.zendesk.maxwell.core.replication.Position;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Optional;
import java.util.Properties;

import static org.mockito.Mockito.*;

public class KinesisCallbackTest {

	@Mock
	private ConfigurationSupport configurationSupport;
	@Mock
	private ProducerExtensionConfigurators producerExtensionConfigurators;

	@Before
	public void init(){
		MockitoAnnotations.initMocks(this);

		ExtensionConfigurator<Producer> configurator = mock(ExtensionConfigurator.class);
		ExtensionConfiguration configuration = mock(ExtensionConfiguration.class);

		when(configurationSupport.fetchOption(anyString(), any(Properties.class), nullable(String.class))).thenCallRealMethod();
		when(configurationSupport.fetchBooleanOption(anyString(), any(Properties.class), nullable(Boolean.class))).thenCallRealMethod();
		when(configurationSupport.fetchLongOption(anyString(), any(Properties.class), nullable(Long.class))).thenCallRealMethod();
		when(configurationSupport.parseMysqlConfig(anyString(), any(Properties.class))).thenCallRealMethod();

		when(producerExtensionConfigurators.getByIdentifier(anyString())).thenReturn(configurator);
		when(configurator.parseConfiguration(isNull())).thenReturn(Optional.of(configuration));
	}

	@Test
	public void shouldIgnoreProducerErrorByDefault() {
		MaxwellContext context = mock(MaxwellContext.class);
		MaxwellConfig config = new MaxwellConfigFactory(configurationSupport, producerExtensionConfigurators).createNewDefaultConfiguration();
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
		MaxwellConfig config = new MaxwellConfigFactory(configurationSupport, producerExtensionConfigurators).createNewDefaultConfiguration();
		config.setIgnoreProducerError(false);
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
