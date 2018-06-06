package com.zendesk.maxwell.core.config;

import com.github.shyiko.mysql.binlog.network.SSLMode;
import com.zendesk.maxwell.api.config.MaxwellMysqlConfig;
import org.apache.http.client.utils.URIBuilder;

import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Created by kaufmannkr on 12/23/15.
 */
public class BaseMaxwellMysqlConfig implements MaxwellMysqlConfig {

	private String host;
	private Integer port;
	private String database;
	private String user;
	private String password;
	private SSLMode sslMode;
	private Map<String, String> jdbcOptions = new HashMap<>();
	private Integer connectTimeoutMS = 5000;

	public BaseMaxwellMysqlConfig() {
		this.setHost(null);
		this.setPort(null);
		this.setDatabase(null);
		this.setUser(null);
		this.setPassword(null);
		this.setSslMode(null);

		this.setJdbcOptions(new HashMap<>());
		this.getJdbcOptions().put("zeroDateTimeBehavior", "convertToNull");
		this.getJdbcOptions().put("connectTimeout", String.valueOf(getConnectTimeoutMS()));
	}

	public BaseMaxwellMysqlConfig(String host, Integer port, String database, String user, String password,
								  SSLMode sslMode) {
		this.setHost(host);
		this.setPort(port);
		this.setDatabase(database);
		this.setUser(user);
		this.setPassword(password);
		this.setSslMode(sslMode);
	}

	private void useSSL(boolean should) {
		this.getJdbcOptions().put("useSSL", String.valueOf(should));
	}

	private void requireSSL(boolean should) {
		this.getJdbcOptions().put("requireSSL", String.valueOf(should));
	}

	private void verifyServerCertificate(boolean should) {
		this.getJdbcOptions().put("verifyServerCertificate", String.valueOf(should));
	}

	public void setJDBCOptions(String opts) {
		if (opts == null)
			return;

		for ( String opt : opts.split("&") ) {
			String[] valueKeySplit = opt.trim().split("=", 2);
			if (valueKeySplit.length == 2) {
				this.getJdbcOptions().put(valueKeySplit[0], valueKeySplit[1]);
			}
		}
	}

	private void setSSLOptions() {
		if (getSslMode() != null && getSslMode() != SSLMode.DISABLED) {
			this.useSSL(true); // for all SSL modes other than DISABLED, use SSL

			this.verifyServerCertificate(false); // default to not verify server cert
			this.requireSSL(false); // default to not require SSL

			this.requireSSL(getSslMode() == SSLMode.REQUIRED || getSslMode() == SSLMode.VERIFY_CA
					|| getSslMode() == SSLMode.VERIFY_IDENTITY);

			this.verifyServerCertificate(getSslMode() == SSLMode.VERIFY_IDENTITY);
		}
		else {
			this.useSSL(false);
		}
	}

	@Override
	public String getConnectionURI(boolean includeDatabase) throws URISyntaxException {
		this.setSSLOptions();

		URIBuilder uriBuilder = new URIBuilder();

		uriBuilder.setScheme("jdbc:mysql");
		uriBuilder.setHost(getHost());
		uriBuilder.setPort(getPort());

		if (getDatabase() != null && includeDatabase) {
			uriBuilder.setPath("/" + getDatabase());
		}

		for (Map.Entry<String, String> jdbcOption : getJdbcOptions().entrySet()) {
			uriBuilder.addParameter(jdbcOption.getKey(), jdbcOption.getValue());
		}

		return uriBuilder.build().toString();
	}

	@Override
	public String getConnectionURI() throws URISyntaxException { return getConnectionURI(true); }

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		MaxwellMysqlConfig that = (MaxwellMysqlConfig) o;
		return Objects.equals(getHost(), that.getHost()) &&
				Objects.equals(getPort(), that.getPort()) &&
				Objects.equals(getDatabase(), that.getDatabase()) &&
				Objects.equals(getUser(), that.getUser()) &&
				Objects.equals(getPassword(), that.getPassword()) &&
				getSslMode() == that.getSslMode() &&
				Objects.equals(getJdbcOptions(), that.getJdbcOptions()) &&
				Objects.equals(getConnectTimeoutMS(), that.getConnectTimeoutMS());
	}

	@Override
	public boolean isSameServerAs(MaxwellMysqlConfig other) {
		return Objects.equals(getHost(), other.getHost()) &&
			Objects.equals(getPort(), other.getPort());
	}

	@Override
	public int hashCode() {
		return Objects
				.hash(getHost(), getPort(), getDatabase(), getUser(), getPassword(), getSslMode(), getJdbcOptions(), getConnectTimeoutMS());
	}

	@Override
	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	@Override
	public Integer getPort() {
		return port;
	}

	public void setPort(Integer port) {
		this.port = port;
	}

	@Override
	public String getDatabase() {
		return database;
	}

	public void setDatabase(String database) {
		this.database = database;
	}

	@Override
	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	@Override
	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	@Override
	public SSLMode getSslMode() {
		return sslMode;
	}

	public void setSslMode(SSLMode sslMode) {
		this.sslMode = sslMode;
	}

	@Override
	public Map<String, String> getJdbcOptions() {
		return jdbcOptions;
	}

	public void setJdbcOptions(Map<String, String> jdbcOptions) {
		this.jdbcOptions = jdbcOptions;
	}

	@Override
	public Integer getConnectTimeoutMS() {
		return connectTimeoutMS;
	}

	public void setConnectTimeoutMS(Integer connectTimeoutMS) {
		this.connectTimeoutMS = connectTimeoutMS;
	}
}
