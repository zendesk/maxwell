package com.zendesk.maxwell.core.support;

import com.github.shyiko.mysql.binlog.network.SSLMode;
import com.zendesk.maxwell.core.MaxwellContext;
import com.zendesk.maxwell.core.config.*;
import com.zendesk.maxwell.core.replication.Position;

import java.net.URISyntaxException;
import java.sql.SQLException;

import static org.mockito.Mockito.mock;

public class MaxwellContextTestSupport {

	public static MaxwellContext buildContext(int port, Position p, MaxwellFilter filter) throws SQLException, URISyntaxException {
		MaxwellConfig config = new MaxwellConfigFactory(mock(MaxwellCommandLineOptions.class), mock(ConfigurationFileParser.class)).createNewDefaultConfiguration();

		config.replicationMysql.host = "127.0.0.1";
		config.replicationMysql.port = port;
		config.replicationMysql.user = "maxwell";
		config.replicationMysql.password = "maxwell";
		config.replicationMysql.sslMode = SSLMode.DISABLED;

		config.maxwellMysql.host = "127.0.0.1";
		config.maxwellMysql.port = port;
		config.maxwellMysql.user = "maxwell";
		config.maxwellMysql.password = "maxwell";
		config.maxwellMysql.sslMode = SSLMode.DISABLED;

		config.databaseName = "maxwell";

		config.filter = filter;
		config.initPosition = p;

		return new MaxwellContext(config);
	}
}
