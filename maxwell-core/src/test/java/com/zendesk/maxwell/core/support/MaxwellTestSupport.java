package com.zendesk.maxwell.core.support;

import com.github.shyiko.mysql.binlog.network.SSLMode;
import com.zendesk.maxwell.core.*;
import com.zendesk.maxwell.core.config.MaxwellConfig;
import com.zendesk.maxwell.core.config.MaxwellConfigFactory;
import com.zendesk.maxwell.core.config.MaxwellFilter;
import com.zendesk.maxwell.core.producer.BufferedProducer;
import com.zendesk.maxwell.core.producer.MaxwellOutputConfig;
import com.zendesk.maxwell.core.replication.Position;
import com.zendesk.maxwell.core.row.RowMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

@Service
public class MaxwellTestSupport {
	private static final Logger LOGGER = LoggerFactory.getLogger(MaxwellTestSupport.class);

	@Autowired
	private MaxwellRunner maxwellRunner;
	@Autowired
	private MaxwellContextFactory maxwellContextFactory;
	@Autowired
	private MaxwellConfigFactory maxwellConfigFactory;

	private void clearSchemaStore(MysqlIsolatedServer mysql) throws Exception {
		mysql.execute("drop database if exists maxwell");
	}

	public List<RowMap> getRowsWithReplicator(final MysqlIsolatedServer mysql, MaxwellFilter filter, final String queries[], final String before[]) throws Exception {
		MaxwellTestSupportCallback callback = new MaxwellTestSupportCallback() {
			@Override
			public void afterReplicatorStart(MysqlIsolatedServer mysql) throws SQLException {
				 mysql.executeList(Arrays.asList(queries));
			}

			@Override
			public void beforeReplicatorStart(MysqlIsolatedServer mysql) throws SQLException {
				if ( before != null)
					mysql.executeList(Arrays.asList(before));
			}
		};

		return getRowsWithReplicator(mysql, filter, callback, Optional.empty());
	}

	public List<RowMap> getRowsWithReplicator(final MysqlIsolatedServer mysql, final MaxwellFilter filter, final MaxwellTestSupportCallback callback, final Optional<MaxwellOutputConfig> optionalOutputConfig) throws Exception {
		final ArrayList<RowMap> list = new ArrayList<>();

		clearSchemaStore(mysql);

		MaxwellConfig config = maxwellConfigFactory.createNewDefaultConfiguration();

		config.maxwellMysql.user = "maxwell";
		config.maxwellMysql.password = "maxwell";
		config.maxwellMysql.host = "localhost";
		config.maxwellMysql.port = mysql.getPort();
		config.maxwellMysql.sslMode = SSLMode.DISABLED;
		config.replicationMysql = config.maxwellMysql;
		final MaxwellOutputConfig outputConfig = optionalOutputConfig.orElseGet(MaxwellOutputConfig::new);

		if ( filter != null ) {
			if ( filter.isDatabaseWhitelist() )
				filter.includeDatabase("test");
			if ( filter.isTableWhitelist() )
				filter.includeTable("boundary");
		}

		config.filter = filter;
		config.bootstrapperType = "sync";
		config.producerType = "buffer";

		callback.beforeReplicatorStart(mysql);

		config.initPosition = MysqlIsolatedServerTestSupport.capture(mysql.getConnection());
		final String waitObject = "";

		final MaxwellContext maxwellContext = maxwellContextFactory.createFor(config);
		maxwellContext.configureOnReplicationStartEventHandler((c) -> {
			synchronized(waitObject) {
				waitObject.notify();
			}
		});
		maxwellContext.configureOnExecutionCompletedEventHandler((c) -> {
			synchronized(waitObject) {
				waitObject.notify();
			}
		});

		Runnable runnable = () -> maxwellRunner.run(maxwellContext);
		new Thread(runnable).start();

		synchronized(waitObject) { waitObject.wait(); }

		Exception maxwellError = maxwellContext.getError();
		if (maxwellError != null) {
			throw maxwellContext.getError();
		}

		callback.afterReplicatorStart(mysql);
		long finalHeartbeat = maxwellContext.getPositionStore().heartbeat();

		LOGGER.debug("running replicator up to heartbeat: " + finalHeartbeat);

		Long pollTime = 2000L;
		Position lastPositionRead = null;

		for ( ;; ) {
			RowMap row = pollRowFromBufferedProducer(maxwellContext, pollTime);
			pollTime = 500L; // after the first row is receive, we go into a tight loop.

			if ( row == null ) {
				LOGGER.debug("timed out waiting for final row.  Last position we saw: " + lastPositionRead);
				break;
			}

			lastPositionRead = row.getPosition();

			if ( lastPositionRead.getLastHeartbeatRead() >= finalHeartbeat ) {
				// consume whatever's left over in the buffer.
				for ( ;; ) {
					RowMap r = pollRowFromBufferedProducer(maxwellContext, 100);
					if ( r == null )
						break;

					if ( r.toJSON(outputConfig) != null )
						list.add(r);
				}

				break;
			}
			if ( row.toJSON(outputConfig) != null )
				list.add(row);
		}

		callback.beforeTerminate(mysql);
		maxwellRunner.terminate(maxwellContext);

		maxwellError = maxwellContext.getError();
		if (maxwellError != null) {
			throw maxwellError;
		}

		return list;
	}

	public RowMap pollRowFromBufferedProducer(MaxwellContext context, long ms) throws IOException, InterruptedException {
		BufferedProducer p = (BufferedProducer) context.getProducer();
		return p.poll(ms, TimeUnit.MILLISECONDS);
	}
}
