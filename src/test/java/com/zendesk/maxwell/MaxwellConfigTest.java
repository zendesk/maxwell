package com.zendesk.maxwell;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.zendesk.maxwell.producer.AbstractProducer;
import com.zendesk.maxwell.producer.ProducerFactory;
import com.zendesk.maxwell.producer.StdoutProducer;

import org.junit.Test;

import java.nio.file.Paths;

import joptsimple.OptionException;

public class MaxwellConfigTest
{
	private MaxwellConfig config;
	
	@Test
	public void testFetchProducerFactoryFromArgs() {
		config = new MaxwellConfig(new String[] { "--custom_producer.factory=" + TestProducerFactory.class.getName() });
		assertNotNull(config.producerFactory);
		assertTrue(config.producerFactory instanceof TestProducerFactory);
	}
	
	@Test
	public void testFetchProducerFactoryFromConfigFile() throws Exception {
		String configPath = getTestConfigDir() + "producer-factory-config.properties";
		assertNotNull("Config file not found at: " + configPath, Paths.get(configPath));
		
		config = new MaxwellConfig(new String[] { "--config=" + configPath });
		assertNotNull(config.producerFactory);
		assertTrue(config.producerFactory instanceof TestProducerFactory);
	}
	
	@Test(expected = OptionException.class)
	public void testCustomProperties() {
		// custom properties are not supported on the command line just like 'kafka.*' properties
		new MaxwellConfig(new String[] { "--custom.foo=bar" });
	}
	
	@Test
	public void testCustomPropertiesFromConfigFile() throws Exception {
		String configPath = getTestConfigDir() + "producer-factory-config.properties";
		assertNotNull("Config file not found at: " + configPath, Paths.get(configPath));
		
		config = new MaxwellConfig(new String[] { "--config=" + configPath });
		assertEquals("bar", config.customProducerProperties.getProperty("foo"));
	}
	
	private String getTestConfigDir() {
		return System.getProperty("user.dir") + "/src/test/resources/config/";
	}
	
	public static class TestProducerFactory implements ProducerFactory {
		public AbstractProducer createProducer(MaxwellContext context) {
			return new StdoutProducer(context);
		}
	}
}

  