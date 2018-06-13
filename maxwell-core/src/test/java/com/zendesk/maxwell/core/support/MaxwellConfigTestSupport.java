package com.zendesk.maxwell.core.support;

import com.github.shyiko.mysql.binlog.network.SSLMode;
import com.zendesk.maxwell.core.config.MaxwellConfig;
import com.zendesk.maxwell.core.config.MaxwellFilter;
import com.zendesk.maxwell.core.replication.Position;
import com.zendesk.maxwell.core.MaxwellContextFactory;
import com.zendesk.maxwell.core.MaxwellContext;
import com.zendesk.maxwell.core.config.MaxwellMysqlConfig;
import com.zendesk.maxwell.core.config.MaxwellConfigFactory;
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

	public MaxwellConfig createDefaultConfigurationWithBufferedProducerFor(int port, Position p, MaxwellFilter filter){
		MaxwellConfig config = buildConfig(port, p, filter);
		config.setProducerType("buffer");
		return config;
	}

	private MaxwellConfig buildConfig(int port, Position p, MaxwellFilter filter) {
		MaxwellConfig config = maxwellConfigFactory.create();

		MaxwellMysqlConfig replicationMysql = config.getReplicationMysql();
		replicationMysql.setHost("127.0.0.1");
		replicationMysql.setPort(port);
		replicationMysql.setUser("maxwell");
		replicationMysql.setPassword("maxwell");
		replicationMysql.setSslMode(SSLMode.DISABLED);

		MaxwellMysqlConfig maxwellMysql = config.getMaxwellMysql();
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

	public MaxwellContext buildContextWithBufferedProducerFor(int port, Position p, MaxwellFilter filter) throws SQLException, URISyntaxException {
		MaxwellConfig config = createDefaultConfigurationWithBufferedProducerFor(port, p, filter);
		return maxwellContextFactory.createFor(config);
	}

}
