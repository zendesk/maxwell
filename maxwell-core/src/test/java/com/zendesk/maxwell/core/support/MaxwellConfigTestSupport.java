package com.zendesk.maxwell.core.support;

import com.github.shyiko.mysql.binlog.network.SSLMode;
import com.zendesk.maxwell.core.MaxwellContext;
import com.zendesk.maxwell.core.MaxwellContextFactory;
import com.zendesk.maxwell.core.config.*;
import com.zendesk.maxwell.core.replication.Position;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.net.URISyntaxException;
import java.sql.SQLException;

@Service
public class MaxwellConfigTestSupport {

	private final MaxwellConfigFactory maxwellConfigFactory;
	private final MaxwellContextFactory maxwellContextFactory;

	@Autowired
	public MaxwellConfigTestSupport(MaxwellConfigFactory maxwellConfigFactory, MaxwellContextFactory maxwellContextFactory) {
		this.maxwellConfigFactory = maxwellConfigFactory;
		this.maxwellContextFactory = maxwellContextFactory;
	}

	private MaxwellConfig buildConfig(int port, Position p, MaxwellFilter filter) {
		MaxwellConfig config = maxwellConfigFactory.createNewDefaultConfiguration();

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
		return config;
	}

	public MaxwellContext buildContext(int port, Position p, MaxwellFilter filter) throws SQLException, URISyntaxException {
		MaxwellConfig config = buildConfig(port, p, filter);
		return maxwellContextFactory.createFor(config);
	}
}
