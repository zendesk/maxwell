package com.zendesk.maxwell.core.config;

import com.github.shyiko.mysql.binlog.network.SSLMode;

import java.net.URISyntaxException;
import java.util.Map;

public interface MaxwellMysqlConfig {
    String CONFIGURATION_OPTION_HOST = "host";
    String CONFIGURATION_OPTION_PASSWORD = "password";
    String CONFIGURATION_OPTION_USER = "user";
    String CONFIGURATION_OPTION_PORT = "port";
    String CONFIGURATION_OPTION_SSL = "ssl";
    String CONFIGURATION_OPTION_JDBC_OPTIONS = "jdbc_options";

	String CONFIGURATION_OPTION_PREFIX_REPLICATION = "replication_";
	String CONFIGURATION_OPTION_PREFIX_SCHEMA = "schema_";

	int DEFAULT_MYSQL_PORT = 3306;

	String getConnectionURI(boolean includeDatabase) throws URISyntaxException;

	String getConnectionURI() throws URISyntaxException;

	boolean isSameServerAs(MaxwellMysqlConfig other);

	String getHost();

	Integer getPort();

	String getDatabase();

	String getUser();

	String getPassword();

	SSLMode getSslMode();

	Map<String, String> getJdbcOptions();

	Integer getConnectTimeoutMS();
}
