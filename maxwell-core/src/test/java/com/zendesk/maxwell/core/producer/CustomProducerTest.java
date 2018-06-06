package com.zendesk.maxwell.core.producer;

import com.codahale.metrics.MetricRegistry;
import com.zendesk.maxwell.api.MaxwellContext;
import com.zendesk.maxwell.api.config.MaxwellConfig;
import com.zendesk.maxwell.api.monitoring.Metrics;
import com.zendesk.maxwell.api.producer.Producer;
import com.zendesk.maxwell.api.producer.ProducerContext;
import com.zendesk.maxwell.api.producer.ProducerFactory;
import com.zendesk.maxwell.api.row.RowMap;
import com.zendesk.maxwell.core.MaxwellSystemContext;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.Collections;
import java.util.Properties;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.*;

public class CustomProducerTest {

	private Producers sut;

	@Before
	public void init(){
		sut = new Producers(Collections.EMPTY_LIST);
	}
	
	@Test
	public void shouldSetupCustomProducerWithConfiguration() throws Exception {
		MaxwellConfig config = mock(MaxwellConfig.class);
		MetricRegistry metricRegistry = mock(MetricRegistry.class);
		Metrics metrics = mock(Metrics.class);
		MaxwellSystemContext context = mock(MaxwellSystemContext.class);
		Properties properties = mock(Properties.class);

		when(config.getCustomProducerFactory()).thenReturn(TestProducerFactory.class.getName());
		when(metrics.getRegistry()).thenReturn(metricRegistry);
		when(context.getMetrics()).thenReturn(metrics);
		when(context.getConfig()).thenReturn(config);

		sut.createAndRegister(context, properties);

		ArgumentCaptor<ProducerContext> producerContextArgumentCaptor = ArgumentCaptor.forClass(ProducerContext.class);
		verify(context).setProducerContext(producerContextArgumentCaptor.capture());

		ProducerContext producerContext = producerContextArgumentCaptor.getValue();
		assertNotNull(producerContext);
		assertThat(producerContext.getProducer(), instanceOf(TestProducer.class));
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
