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
		BaseMaxwellConfig config = maxwellConfigFactory.createNewDefaultConfiguration();

		config.getReplicationMysql().setHost("127.0.0.1");
		config.getReplicationMysql().setPort(port);
		config.getReplicationMysql().setUser("maxwell");
		config.getReplicationMysql().setPassword("maxwell");
		config.getReplicationMysql().setSslMode(SSLMode.DISABLED);

		config.getMaxwellMysql().setHost("127.0.0.1");
		config.getMaxwellMysql().setPort(port);
		config.getMaxwellMysql().setUser("maxwell");
		config.getMaxwellMysql().setPassword("maxwell");
		config.getMaxwellMysql().setSslMode(SSLMode.DISABLED);

		config.setDatabaseName("maxwell");

		config.setFilter(filter);
		config.setInitPosition(p);
		return config;
	}

	public MaxwellContext buildContext(int port, Position p, MaxwellFilter filter) throws SQLException, URISyntaxException {
		MaxwellConfig config = buildConfig(port, p, filter);
		return maxwellContextFactory.createFor(config);
	}
}
