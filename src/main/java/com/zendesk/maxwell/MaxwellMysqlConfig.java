package com.zendesk.maxwell;

import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import com.github.shyiko.mysql.binlog.network.SSLMode;

import org.apache.http.client.utils.URIBuilder;

/**
 * Configuration object describing a mysql connection
 */
public class MaxwellMysqlConfig {

	public String host;
	public Integer port;
	public String database;
	public String user;
	public String password;
	public SSLMode sslMode;
	/**
	 * determines whether we enable replication heartbeats.  Different from maxwell heartbeats
	 */
	public boolean enableHeartbeat;
	/**
	 * Hashmap of key/value JDBC driver options.  Not used for replication connections, generally.
	 */
	public Map<String, String> jdbcOptions;
	/**
	 * Connection timeout for JDBC connections
	 */
	public Integer connectTimeoutMS = 5000;

	/**
	 * Instantiate a default connection config
	 */
	public MaxwellMysqlConfig() {
		this.host = null;
		this.port = null;
		this.database = null;
		this.user = null;
		this.password = null;
		this.sslMode = null;
		this.enableHeartbeat = false;

		this.jdbcOptions = new HashMap<>();
		this.jdbcOptions.put("zeroDateTimeBehavior", "convertToNull");
		this.jdbcOptions.put("connectTimeout", String.valueOf(connectTimeoutMS));
		this.jdbcOptions.put("allowPublicKeyRetrieval", "true");
	}

	/**
	 * Instantiate a mysql connection config
	 * @param host Mysql Host
	 * @param port Mysql port
	 * @param database Database name
	 * @param user User
	 * @param password Password
	 * @param sslMode SSL connection mode
	 * @param enableHeartbeat Replication heartbeats
	 */
	public MaxwellMysqlConfig(String host, Integer port, String database, String user, String password,
			SSLMode sslMode, boolean enableHeartbeat) {
		this();
		this.host = host;
		this.port = port;
		this.database = database;
		this.user = user;
		this.password = password;
		this.sslMode = sslMode;
		this.enableHeartbeat = enableHeartbeat;
	}

	/**
	 * Clone a mysql config
	 * @param c the config to clone
	 */
	public MaxwellMysqlConfig(MaxwellMysqlConfig c) {
		this();
		this.host = c.host;
		this.port = c.port;
		this.database = c.database;
		this.user = c.user;
		this.password = c.password;
		this.sslMode = c.sslMode;
	}

	private void useSSL(boolean should) {
		this.jdbcOptions.put("useSSL", String.valueOf(should));
	}

	private void requireSSL(boolean should) {
		this.jdbcOptions.put("requireSSL", String.valueOf(should));
	}

	private void verifyServerCertificate(boolean should) {
		this.jdbcOptions.put("verifyServerCertificate", String.valueOf(should));
	}

	/**
	 * Parse JDBC options from a key=val&amp;key2=val2 string
	 * @param opts string to parse
	 */
	public void setJDBCOptions(String opts) {
		if (opts == null)
			return;

		for ( String opt : opts.split("&") ) {
			String[] valueKeySplit = opt.trim().split("=", 2);
			if (valueKeySplit.length == 2) {
				this.jdbcOptions.put(valueKeySplit[0], valueKeySplit[1]);
			}
		}
	}

	private void setSSLOptions() {
		if (sslMode != null && sslMode != SSLMode.DISABLED) {
			this.useSSL(true); // for all SSL modes other than DISABLED, use SSL

			this.verifyServerCertificate(false); // default to not verify server cert
			this.requireSSL(false); // default to not require SSL

			this.requireSSL(sslMode == SSLMode.REQUIRED || sslMode == SSLMode.VERIFY_CA
					|| sslMode == SSLMode.VERIFY_IDENTITY);

			this.verifyServerCertificate(sslMode == SSLMode.VERIFY_IDENTITY);
		}
		else {
			this.useSSL(false);
		}
	}

	/**
	 * Build a connection URI from the config
	 * @param includeDatabase whether to include the database name in th euri
	 * @return a connection URI string
	 * @throws URISyntaxException if we have problems building the URI
	 */
	public String getConnectionURI(boolean includeDatabase) throws URISyntaxException {
		this.setSSLOptions();

		URIBuilder uriBuilder = new URIBuilder();

		uriBuilder.setScheme("jdbc:mysql");
		uriBuilder.setHost(host);
		uriBuilder.setPort(port);

		if (database != null && includeDatabase) {
			uriBuilder.setPath("/" + database);
		}

		for (Map.Entry<String, String> jdbcOption : jdbcOptions.entrySet()) {
			uriBuilder.addParameter(jdbcOption.getKey(), jdbcOption.getValue());
		}

		// added by d8888 2018/09/10, force JDBC to use UTF-8 to support using non-english db, table & column names
		uriBuilder.addParameter("characterEncoding", "UTF-8");
		uriBuilder.addParameter("tinyInt1isBit", "false");

		return uriBuilder.build().toString();
	}

	/**
	 * Build a connection URI from the config
	 * @return a connection URI
	 * @throws URISyntaxException if we have problems
	 */
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
		return Objects.equals(host, that.host) &&
				Objects.equals(port, that.port) &&
				Objects.equals(database, that.database) &&
				Objects.equals(user, that.user) &&
				Objects.equals(password, that.password) &&
				sslMode == that.sslMode &&
				Objects.equals(jdbcOptions, that.jdbcOptions) &&
				Objects.equals(connectTimeoutMS, that.connectTimeoutMS);
	}

	public boolean sameServerAs(MaxwellMysqlConfig other) {
		return Objects.equals(host, other.host) &&
			Objects.equals(port, other.port);
	}

	@Override
	public int hashCode() {
		return Objects
				.hash(host, port, database, user, password, sslMode, jdbcOptions, connectTimeoutMS);
	}
}
