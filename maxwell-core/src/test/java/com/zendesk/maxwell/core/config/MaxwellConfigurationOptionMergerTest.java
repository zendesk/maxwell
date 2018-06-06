package com.zendesk.maxwell.core.config;

import com.zendesk.maxwell.core.SpringTestContextConfiguration;
import joptsimple.OptionException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.nio.file.Paths;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {SpringTestContextConfiguration.class})
public class MaxwellConfigurationOptionMergerTest {
	@Rule
	public final EnvironmentVariables environmentVariables = new EnvironmentVariables();

	@Autowired
	private MaxwellConfigurationOptionMerger sut;

	@Test(expected = OptionException.class)
	public void testCustomProperties() {
		// custom properties are not supported on the command line just like 'kafka.*' properties
		sut.merge(new String[] { "--custom.foo=bar" });
	}

	@Test
	public void testCustomPropertiesFromConfigFile() {
		String configPath = getTestConfigDir() + "producer-factory-config.properties";
		assertNotNull("Config file not found at: " + configPath, Paths.get(configPath));

		Properties result = sut.merge(new String[] { "--config=" + configPath });
		assertEquals("bar", result.getProperty("custom_producer.foo"));
	}

	@Test
	public void testEnvVarConfigViaOption() {
		environmentVariables.set("MAXWELL_USER", "foo");
		environmentVariables.set("maxwell_password", "bar");
		environmentVariables.set("maxwell_host", "remotehost");
		environmentVariables.set("MAXWELL_KAFKA.RETRIES", "100");
		environmentVariables.set("MAXWELL_PRODUCER", "kafka");
		environmentVariables.set("USER", "mysql");

		Properties result = sut.merge(new String[] { "--env_config_prefix=MAXWELL_", "--host=localhost" });
		assertEquals("foo", result.getProperty("user"));
		assertEquals("bar", result.getProperty("password"));
		assertEquals("localhost", result.getProperty("host"));
		assertEquals("100", result.getProperty("kafka.retries"));
	}

	@Test
	public void testEnvVarConfigViaConfigFile() {
		environmentVariables.set("FOO_USER", "foo");
		environmentVariables.set("foo_password", "bar");
		environmentVariables.set("foo_host", "remotehost");
		environmentVariables.set("FOO_KAFKA.RETRIES", "100");
		environmentVariables.set("FOO_PRODUCER", "kafka");
		environmentVariables.set("USER", "mysql");
		String configPath = getTestConfigDir() + "env-var-config.properties";
		assertNotNull("Config file not found at: " + configPath, Paths.get(configPath));

		Properties result = sut.merge(new String[] { "--config=" + configPath, "--host=localhost" });
		assertEquals("foo", result.getProperty("user"));
		assertEquals("bar", result.getProperty("password"));
		assertEquals("localhost", result.getProperty("host"));
		assertEquals("100", result.getProperty("kafka.retries"));
	}

	private String getTestConfigDir() {
		return System.getProperty("user.dir") + "/src/test/resources/config/";
	}


}
