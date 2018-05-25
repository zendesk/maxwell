package com.zendesk.maxwell.core;

import com.zendesk.maxwell.core.config.MaxwellConfig;
import com.zendesk.maxwell.core.config.MaxwellConfigFactory;
import com.zendesk.maxwell.core.producer.AbstractProducer;
import com.zendesk.maxwell.core.producer.ProducerFactory;
import com.zendesk.maxwell.core.producer.StdoutProducer;
import joptsimple.OptionException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.nio.file.Paths;

import static org.junit.Assert.*;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {SpringTestContextConfiguration.class})
public class MaxwellConfigurationFactoryTest {
	private MaxwellConfig config;

	@Rule
	public final EnvironmentVariables environmentVariables = new EnvironmentVariables();
	
	@Autowired
	private MaxwellConfigFactory maxwellConfigFactory;
	
	@Test
	public void testFetchProducerFactoryFromArgs() {
		config = maxwellConfigFactory.createConfigurationFromArgumentsAndConfigurationFileAndEnvironmentVariables(new String[] { "--custom_producer.factory=" + TestProducerFactory.class.getName() });
		assertNotNull(config.producerFactory);
		assertTrue(config.producerFactory instanceof TestProducerFactory);
	}
	
	@Test
	public void testFetchProducerFactoryFromConfigFile() {
		String configPath = getTestConfigDir() + "producer-factory-config.properties";
		assertNotNull("Config file not found at: " + configPath, Paths.get(configPath));
		
		config = maxwellConfigFactory.createConfigurationFromArgumentsAndConfigurationFileAndEnvironmentVariables(new String[] { "--config=" + configPath });
		assertNotNull(config.producerFactory);
		assertTrue(config.producerFactory instanceof TestProducerFactory);
	}
	
	@Test(expected = OptionException.class)
	public void testCustomProperties() {
		// custom properties are not supported on the command line just like 'kafka.*' properties
		maxwellConfigFactory.createConfigurationFromArgumentsAndConfigurationFileAndEnvironmentVariables(new String[] { "--custom.foo=bar" });
	}
	
	@Test
	public void testCustomPropertiesFromConfigFile() {
		String configPath = getTestConfigDir() + "producer-factory-config.properties";
		assertNotNull("Config file not found at: " + configPath, Paths.get(configPath));
		
		config = maxwellConfigFactory.createConfigurationFromArgumentsAndConfigurationFileAndEnvironmentVariables(new String[] { "--config=" + configPath });
		assertEquals("bar", config.customProducerProperties.getProperty("foo"));
	}

	@Test
	public void testEnvVarConfigViaOption() {
		environmentVariables.set("MAXWELL_USER", "foo");
		environmentVariables.set("maxwell_password", "bar");
		environmentVariables.set("maxwell_host", "remotehost");
		environmentVariables.set("MAXWELL_KAFKA.RETRIES", "100");
		environmentVariables.set("USER", "mysql");
		config = maxwellConfigFactory.createConfigurationFromArgumentsAndConfigurationFileAndEnvironmentVariables(new String[] { "--env_config_prefix=MAXWELL_", "--host=localhost" });
		assertEquals("foo", config.maxwellMysql.user);
		assertEquals("bar", config.maxwellMysql.password);
		assertEquals("localhost", config.maxwellMysql.host);
		assertEquals("100", config.kafkaProperties.getProperty("retries"));
	}

	@Test
	public void testEnvVarConfigViaConfigFile() {
		environmentVariables.set("FOO_USER", "foo");
		environmentVariables.set("foo_password", "bar");
		environmentVariables.set("foo_host", "remotehost");
		environmentVariables.set("FOO_KAFKA.RETRIES", "100");
		environmentVariables.set("USER", "mysql");
		String configPath = getTestConfigDir() + "env-var-config.properties";
		assertNotNull("Config file not found at: " + configPath, Paths.get(configPath));

		config = maxwellConfigFactory.createConfigurationFromArgumentsAndConfigurationFileAndEnvironmentVariables(new String[] { "--config=" + configPath, "--host=localhost" });
		assertEquals("foo", config.maxwellMysql.user);
		assertEquals("bar", config.maxwellMysql.password);
		assertEquals("localhost", config.maxwellMysql.host);
		assertEquals("100", config.kafkaProperties.getProperty("retries"));
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
