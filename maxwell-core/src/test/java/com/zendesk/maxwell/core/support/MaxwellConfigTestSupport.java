package com.zendesk.maxwell.core.support;

import com.github.shyiko.mysql.binlog.network.SSLMode;
import com.zendesk.maxwell.core.MaxwellContext;
import com.zendesk.maxwell.core.MaxwellContextFactory;
import com.zendesk.maxwell.core.config.*;
import com.zendesk.maxwell.api.replication.Position;
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

	private MaxwellConfig buildConfig(int port, Position p, BaseMaxwellFilter filter) {
		BaseMaxwellConfig config = maxwellConfigFactory.createNewDefaultConfiguration();

		BaseMaxwellMysqlConfig replicationMysql = (BaseMaxwellMysqlConfig) config.getReplicationMysql();
		replicationMysql.setHost("127.0.0.1");
		replicationMysql.setPort(port);
		replicationMysql.setUser("maxwell");
		replicationMysql.setPassword("maxwell");
		replicationMysql.setSslMode(SSLMode.DISABLED);

		BaseMaxwellMysqlConfig maxwellMysql = (BaseMaxwellMysqlConfig) config.getMaxwellMysql();
		maxwellMysql.setHost("127.0.0.1");
		maxwellMysql.setPort(port);
		maxwellMysql.setUser("maxwell");
		maxwellMysql.setPassword("maxwell");
		maxwellMysql.setSslMode(SSLMode.DISABLED);

		config.setDatabaseName("maxwell");

		config.setFilter(filter);
		config.setInitPosition(p);
		return config;
	}

	public MaxwellContext buildContext(int port, Position p, BaseMaxwellFilter filter) throws SQLException, URISyntaxException {
		MaxwellConfig config = buildConfig(port, p, filter);
		return maxwellContextFactory.createFor(config);
	}
}
