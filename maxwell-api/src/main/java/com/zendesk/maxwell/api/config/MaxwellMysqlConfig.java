package com.zendesk.maxwell.api.config;

import com.github.shyiko.mysql.binlog.network.SSLMode;

import java.net.URISyntaxException;
import java.util.Map;

public interface MaxwellMysqlConfig {
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
