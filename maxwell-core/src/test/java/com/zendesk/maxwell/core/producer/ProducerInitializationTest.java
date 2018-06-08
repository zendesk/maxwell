package com.zendesk.maxwell.core.producer;

import com.codahale.metrics.MetricRegistry;
import com.zendesk.maxwell.api.MaxwellContext;
import com.zendesk.maxwell.api.config.MaxwellConfig;
import com.zendesk.maxwell.api.monitoring.Metrics;
import com.zendesk.maxwell.api.producer.AbstractProducer;
import com.zendesk.maxwell.api.producer.Producer;
import com.zendesk.maxwell.api.producer.ProducerFactory;
import com.zendesk.maxwell.api.row.RowMap;
import com.zendesk.maxwell.core.MaxwellSystemContext;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.context.ApplicationContext;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.*;

public class ProducerInitializationTest {

	private ApplicationContext applicationContext;
	private ProducerInitialization sut;

	@Before
	public void init(){
		applicationContext = mock(ApplicationContext.class);
		sut = new ProducerInitialization(applicationContext);
	}
	
	@Test
	public void shouldSetupProducerFromSpringContext() throws Exception {
		MaxwellConfig config = mock(MaxwellConfig.class);
		MetricRegistry metricRegistry = mock(MetricRegistry.class);
		Metrics metrics = mock(Metrics.class);
		MaxwellSystemContext context = mock(MaxwellSystemContext.class);

		when(config.getProducerFactory()).thenReturn(TestProducerFactory.class.getName());
		when(metrics.getRegistry()).thenReturn(metricRegistry);
		when(context.getMetrics()).thenReturn(metrics);
		when(context.getConfig()).thenReturn(config);
		when(applicationContext.getBean(TestProducerFactory.class)).thenReturn(new TestProducerFactory());

		sut.createAndRegister(context);

		ArgumentCaptor<Producer> producerArgumentCaptor = ArgumentCaptor.forClass(Producer.class);
		verify(context).setProducer(producerArgumentCaptor.capture());

		Producer producer = producerArgumentCaptor.getValue();
		assertThat(producer, instanceOf(TestProducer.class));
	}

	@Test
	public void shouldSetupProducerThroughClassInstantiation() throws Exception {
		MaxwellConfig config = mock(MaxwellConfig.class);
		MetricRegistry metricRegistry = mock(MetricRegistry.class);
		Metrics metrics = mock(Metrics.class);
		MaxwellSystemContext context = mock(MaxwellSystemContext.class);

		when(config.getProducerFactory()).thenReturn(TestProducerFactory.class.getName());
		when(metrics.getRegistry()).thenReturn(metricRegistry);
		when(context.getMetrics()).thenReturn(metrics);
		when(context.getConfig()).thenReturn(config);
		when(applicationContext.getBean(TestProducerFactory.class)).thenThrow(new NoSuchBeanDefinitionException("Test"));

		sut.createAndRegister(context);

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
		public TestProducer(MaxwellContext context) {
			super(context);
		}

		@Override
		public void push(RowMap r) throws Exception {

		}
	}
}
