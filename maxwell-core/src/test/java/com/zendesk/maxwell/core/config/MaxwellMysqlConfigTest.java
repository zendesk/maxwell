package com.zendesk.maxwell.core.config;

import com.github.shyiko.mysql.binlog.network.SSLMode;
import org.junit.Test;

import java.net.URISyntaxException;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class MaxwellMysqlConfigTest {

	@Test
	public void testBasicUri() {

		BaseMaxwellMysqlConfig config = new BaseMaxwellMysqlConfig();

		config.setHost("localhost");
		config.setPort(3306);
		config.setUser("maxwell");
		config.setPassword("maxwell");
		config.setDatabase("maxwell");
		config.setSslMode(SSLMode.DISABLED);
		config.setJDBCOptions("autoReconnect=true&initialTimeout=2&maxReconnects=10");

		try {
			final String uri = config.getConnectionURI();
			assertThat(uri, is(equalTo("jdbc:mysql://localhost:3306/maxwell?connectTimeout=5000&zeroDateTimeBehavior=convertToNull&initialTimeout=2&autoReconnect=true&maxReconnects=10&useSSL=false")));
		}
		catch (URISyntaxException e) {
			fail();
		}

	}

	@Test
	public void testSSLPreferred() {

		BaseMaxwellMysqlConfig config = new BaseMaxwellMysqlConfig();

		config.setHost("localhost");
		config.setPort(3306);
		config.setUser("maxwell");
		config.setPassword("maxwell");
		config.setDatabase("maxwell");
		config.setSslMode(SSLMode.PREFERRED);

		try {
			final String uri = config.getConnectionURI();
			assertThat(uri, is(containsString("useSSL=true")));
			assertThat(uri, is(containsString("requireSSL=false")));
			assertThat(uri, is(containsString("verifyServerCertificate=false")));
		}
		catch (URISyntaxException e) {
			fail();
		}

	}

	@Test
	public void testSSLRequired() {

		BaseMaxwellMysqlConfig config = new BaseMaxwellMysqlConfig();

		config.setHost("localhost");
		config.setPort(3306);
		config.setUser("maxwell");
		config.setPassword("maxwell");
		config.setDatabase("maxwell");
		config.setSslMode(SSLMode.REQUIRED);

		try {
			String uri = config.getConnectionURI();
			assertThat(uri, is(containsString("requireSSL=true")));
			assertThat(uri, is(containsString("useSSL=true")));
			assertThat(uri, is(containsString("verifyServerCertificate=false")));
		}
		catch (URISyntaxException e) {
			fail();
		}

	}

	@Test
	public void testSSLVerifyCA() {

		BaseMaxwellMysqlConfig config = new BaseMaxwellMysqlConfig();

		config.setHost("localhost");
		config.setPort(3306);
		config.setUser("maxwell");
		config.setPassword("maxwell");
		config.setDatabase("maxwell");
		config.setSslMode(SSLMode.VERIFY_CA);

		try {
			final String uri = config.getConnectionURI();
			assertThat(uri, is(containsString("requireSSL=true")));
			assertThat(uri, is(containsString("useSSL=true")));
			assertThat(uri, is(containsString("verifyServerCertificate=false")));
		}
		catch (URISyntaxException e) {
			fail();
		}

	}

	@Test
	public void testSSLVerifyId() {

		BaseMaxwellMysqlConfig config = new BaseMaxwellMysqlConfig();

		config.setHost("localhost");
		config.setPort(3306);
		config.setUser("maxwell");
		config.setPassword("maxwell");
		config.setDatabase("maxwell");
		config.setSslMode(SSLMode.VERIFY_IDENTITY);

		try {
			final String uri = config.getConnectionURI();
			assertThat(uri, is(containsString("requireSSL=true")));
			assertThat(uri, is(containsString("useSSL=true")));
			assertThat(uri, is(containsString("verifyServerCertificate=true")));
		}
		catch (URISyntaxException e) {
			fail();
		}

	}

}
