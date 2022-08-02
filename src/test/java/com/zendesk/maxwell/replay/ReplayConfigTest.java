package com.zendesk.maxwell.replay;

import com.zendesk.maxwell.MaxwellTestSupport;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * @author udyr@shlaji.com
 */
public class ReplayConfigTest {

	@Rule
	public final EnvironmentVariables environmentVariables = new EnvironmentVariables();

	@Test
	public void testBinlogFiles() {
		String binlogPath = MaxwellTestSupport.getSQLDir() + "replay/binlog.000004";
		environmentVariables.set("MAXWELL_USER", "foo");
		environmentVariables.set("maxwell_password", "bar");
		environmentVariables.set("maxwell_host", "remotehost");
		environmentVariables.set("MAXWELL_KAFKA.RETRIES", "100");
		environmentVariables.set("USER", "mysql");
		environmentVariables.set("MAXWELL_replay_binlog", binlogPath);
		ReplayConfig config = new ReplayConfig(new String[]{"--env_config_prefix=MAXWELL_", "--host=localhost"});
		config.validate();

		assertFalse(config.binlogFiles.isEmpty());

		assertEquals("foo", config.replicationMysql.user);
		assertEquals("bar", config.replicationMysql.password);
		assertEquals("localhost", config.replicationMysql.host);

		assertEquals("foo", config.maxwellMysql.user);
		assertEquals("bar", config.maxwellMysql.password);
		assertEquals("localhost", config.maxwellMysql.host);

		assertEquals("100", config.kafkaProperties.getProperty("retries"));
	}
}
