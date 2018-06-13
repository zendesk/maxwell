package com.zendesk.maxwell.core.producer;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.zendesk.maxwell.core.MaxwellContext;
import com.zendesk.maxwell.core.config.MaxwellConfig;
import com.zendesk.maxwell.core.monitoring.MaxwellDiagnosticRegistry;
import com.zendesk.maxwell.core.monitoring.Metrics;
import com.zendesk.maxwell.core.row.RowMap;
import com.zendesk.maxwell.core.MaxwellSystemContext;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.Optional;
import java.util.Properties;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.*;

public class ProducerInitializationTest {

	private ProducerConfigurators producerConfigurators;
	private MaxwellDiagnosticRegistry maxwellDiagnosticRegistry;
	private HealthCheckRegistry healthCheckRegistry;

	private ProducerInitialization sut;

	@Before
	public void init(){
		producerConfigurators = mock(ProducerConfigurators.class);
		maxwellDiagnosticRegistry = mock(MaxwellDiagnosticRegistry.class);
		healthCheckRegistry = mock(HealthCheckRegistry.class);

		sut = new ProducerInitialization(producerConfigurators, maxwellDiagnosticRegistry, healthCheckRegistry);
	}
	
	@Test
	public void shouldSetupProducerFromSpringContext() {
		String producerType = "producerType";
		Properties configurationSettings = mock(Properties.class);
		MaxwellConfig config = mock(MaxwellConfig.class);
		MetricRegistry metricRegistry = mock(MetricRegistry.class);
		Metrics metrics = mock(Metrics.class);
		MaxwellSystemContext context = mock(MaxwellSystemContext.class);
		ProducerConfigurator producerConfigurator = mock(ProducerConfigurator.class);
		ProducerConfiguration producerConfiguration = mock(ProducerConfiguration.class);
		Producer producer = mock(Producer.class);

		when(config.getProducerType()).thenReturn(producerType);
		when(metrics.getRegistry()).thenReturn(metricRegistry);
		when(context.getMetrics()).thenReturn(metrics);
		when(context.getConfig()).thenReturn(config);
		when(producerConfigurator.parseConfiguration(configurationSettings)).thenReturn(Optional.of(producerConfiguration));
		when(producerConfigurator.configure(context, producerConfiguration)).thenReturn(producer);
		when(producerConfigurators.getByIdentifier(producerType)).thenReturn(producerConfigurator);

		sut.initialize(context, configurationSettings);

		ArgumentCaptor<Producer> producerArgumentCaptor = ArgumentCaptor.forClass(Producer.class);
		verify(context).setProducer(producerArgumentCaptor.capture());

		Producer capturedProducer = producerArgumentCaptor.getValue();
		assertSame(producer, capturedProducer);
	}

	@Test
	public void shouldSetupProducerThroughCustomProducerFactoryClass() {
		Properties configurationSettings = mock(Properties.class);
		MaxwellConfig config = mock(MaxwellConfig.class);
		MetricRegistry metricRegistry = mock(MetricRegistry.class);
		Metrics metrics = mock(Metrics.class);
		MaxwellSystemContext context = mock(MaxwellSystemContext.class);

		when(config.getProducerFactory()).thenReturn(TestProducerFactory.class.getName());
		when(metrics.getRegistry()).thenReturn(metricRegistry);
		when(context.getMetrics()).thenReturn(metrics);
		when(context.getConfig()).thenReturn(config);

		sut.initialize(context, configurationSettings);

		ArgumentCaptor<Producer> producerArgumentCaptor = ArgumentCaptor.forClass(Producer.class);
		verify(context).setProducer(producerArgumentCaptor.capture());

		Producer producer = producerArgumentCaptor.getValue();
		assertThat(producer, instanceOf(TestProducer.class));
	}

	public static class TestProducerFactory implements ProducerFactory {
		public Producer createProducer(MaxwellContext context) {
			return new TestProducer(context);
		}
	}

	public static class TestProducer extends AbstractProducer {
		TestProducer(MaxwellContext context) {
			super(context);
		}

		@Override
		public void push(RowMap r) {

		}
	}
}
