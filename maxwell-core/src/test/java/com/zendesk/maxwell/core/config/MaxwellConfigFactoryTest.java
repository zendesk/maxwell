package com.zendesk.maxwell.core.config;

import com.zendesk.maxwell.core.MaxwellContext;
import com.zendesk.maxwell.core.SpringTestContextConfiguration;
import com.zendesk.maxwell.core.producer.AbstractProducer;
import com.zendesk.maxwell.core.producer.ProducerFactory;
import com.zendesk.maxwell.core.producer.impl.stdout.StdoutProducer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.Properties;

import static org.junit.Assert.*;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {SpringTestContextConfiguration.class})
public class MaxwellConfigFactoryTest {

	@Autowired
	private MaxwellConfigFactory sut;
	
	@Test
	public void shouldSetupCustomProducerWithConfiguration() {
		Properties properties = new Properties();
		properties.put("custom_producer.factory", "com.zendesk.maxwell.core.config.MaxwellConfigFactoryTest$TestProducerFactory");
		properties.put("custom_producer.foo", "bar");

		MaxwellConfig config = sut.createFor(properties);
		assertNotNull(config.getProducerFactory());
		assertTrue(config.getProducerFactory() instanceof TestProducerFactory);
	}

	public static class TestProducerFactory implements ProducerFactory {
		public AbstractProducer createProducer(MaxwellContext context) {
			return new StdoutProducer(context);
		}
	}
}
