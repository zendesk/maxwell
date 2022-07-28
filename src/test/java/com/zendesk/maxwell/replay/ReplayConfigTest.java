package com.zendesk.maxwell.replay;

import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;

import static org.junit.Assert.assertEquals;

/**
 * @author udyr@shlaji.com
 */
public class ReplayConfigTest {

	@Rule
	public final EnvironmentVariables environmentVariables = new EnvironmentVariables();

	@Test
	public void testReplayConfig() {
		environmentVariables.set("MAXWELL_USER", "foo");
		environmentVariables.set("maxwell_password", "bar");
		environmentVariables.set("maxwell_host", "remotehost");
		environmentVariables.set("MAXWELL_KAFKA.RETRIES", "100");
		environmentVariables.set("USER", "mysql");
		ReplayConfig config = new ReplayConfig(new String[]{"--env_config_prefix=MAXWELL_", "--host=localhost"});
		assertEquals("foo", config.maxwellMysql.user);
		assertEquals("bar", config.maxwellMysql.password);
		assertEquals("localhost", config.maxwellMysql.host);
		assertEquals("100", config.kafkaProperties.getProperty("retries"));
	}

	@Test
	public void testBinlogFiles() {
		environmentVariables.set("MAXWELL_USER", "foo");
		environmentVariables.set("maxwell_password", "bar");
		environmentVariables.set("maxwell_host", "remotehost");
		environmentVariables.set("MAXWELL_KAFKA.RETRIES", "100");
		environmentVariables.set("USER", "mysql");
		ReplayConfig config = new ReplayConfig(new String[]{"--env_config_prefix=MAXWELL_", "--host=localhost"});
		assertEquals("foo", config.maxwellMysql.user);
		assertEquals("bar", config.maxwellMysql.password);
		assertEquals("localhost", config.maxwellMysql.host);
		assertEquals("100", config.kafkaProperties.getProperty("retries"));
	}
}
