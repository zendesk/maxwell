package com.zendesk.maxwell.core.producer;

import com.codahale.metrics.MetricRegistry;
import com.zendesk.maxwell.core.MaxwellContext;
import com.zendesk.maxwell.core.MaxwellContextFactory;
import com.zendesk.maxwell.core.SpringTestContextConfiguration;
import com.zendesk.maxwell.core.config.MaxwellConfig;
import com.zendesk.maxwell.core.config.MaxwellConfigFactory;
import com.zendesk.maxwell.core.monitoring.Metrics;
import com.zendesk.maxwell.core.producer.AbstractProducer;
import com.zendesk.maxwell.core.producer.Producer;
import com.zendesk.maxwell.core.producer.ProducerFactory;
import com.zendesk.maxwell.core.row.RowMap;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.Collections;
import java.util.Properties;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CustomProducerTest {

	private ProducerConfigurators sut;

	@Before
	public void init(){
		sut = new ProducerConfigurators(Collections.EMPTY_LIST);
	}
	
	@Test
	public void shouldSetupCustomProducerWithConfiguration() throws Exception {
		MaxwellConfig config = mock(MaxwellConfig.class);
		MetricRegistry metricRegistry = mock(MetricRegistry.class);
		Metrics metrics = mock(Metrics.class);
		MaxwellContext context = mock(MaxwellContext.class);
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
