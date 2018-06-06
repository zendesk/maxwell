package com.zendesk.maxwell.core.support;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.shyiko.mysql.binlog.network.SSLMode;
import com.zendesk.maxwell.core.*;
import com.zendesk.maxwell.core.config.*;
import com.zendesk.maxwell.core.producer.impl.buffered.BufferedProducer;
import com.zendesk.maxwell.core.replication.Position;
import com.zendesk.maxwell.core.row.RowMap;
import com.zendesk.maxwell.core.schema.Schema;
import com.zendesk.maxwell.core.schema.SchemaCapturer;
import com.zendesk.maxwell.core.schema.SchemaStoreSchema;
import com.zendesk.maxwell.core.schema.ddl.ResolvedSchemaChange;
import com.zendesk.maxwell.core.schema.ddl.SchemaChange;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

@Service
public class MaxwellTestSupport {
	private static final Logger LOGGER = LoggerFactory.getLogger(MaxwellTestSupport.class);

	@Autowired
	private MaxwellRunner maxwellRunner;
	@Autowired
	private MaxwellContextFactory maxwellContextFactory;
	@Autowired
	private MaxwellConfigFactory maxwellConfigFactory;
	@Autowired
	private MaxwellConfigTestSupport maxwellConfigTestSupport;

	public static MysqlIsolatedServer setupServer(String extraParams) throws Exception {
		MysqlIsolatedServer server = new MysqlIsolatedServer();
		server.boot(extraParams);

		Connection conn = server.getConnection();
		SchemaStoreSchema.ensureMaxwellSchema(conn, "maxwell");
		conn.createStatement().executeQuery("use maxwell");
		SchemaStoreSchema.upgradeSchemaStoreSchema(conn);
		return server;
	}

	public static MysqlIsolatedServer setupServer() throws Exception {
		return setupServer(null);
	}

	public static void setupSchema(MysqlIsolatedServer server, boolean resetBinlogs) throws Exception {
		List<String> queries = new ArrayList<String>(Arrays.asList(
				"CREATE DATABASE if not exists shard_2",
				"DROP DATABASE if exists shard_1",
				"CREATE DATABASE shard_1",
				"USE shard_1"
		));

		for (File file : new File(getSQLDir() + "/schema").listFiles()) {
			if (!file.getName().endsWith(".sql"))
				continue;

			if (file.getName().contains("sharded"))
				continue;

			byte[] sql = Files.readAllBytes(file.toPath());
			String s = new String(sql);
			if (s != null) {
				queries.add(s);
			}
		}

		String shardedFileName;
		if (server.getVersion().atLeast(server.VERSION_5_6))
			shardedFileName = "sharded.sql";
		else
			shardedFileName = "sharded_55.sql";

		File shardedFile = new File(getSQLDir() + "/schema/" + shardedFileName);
		byte[] sql = Files.readAllBytes(shardedFile.toPath());
		String s = new String(sql);
		if (s != null) {
			queries.add(s);
		}

		if (resetBinlogs)
			queries.add("RESET MASTER");

		server.executeList(queries);
	}

	public static void setupSchema(MysqlIsolatedServer server) throws Exception {
		setupSchema(server, true);
	}

	public static String getSQLDir() {
		final String dir = System.getProperty("user.dir");
		return dir + "/src/test/resources/sql/";
	}


	private static void clearSchemaStore(MysqlIsolatedServer mysql) throws Exception {
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

	public static boolean inGtidMode() {
		return MysqlIsolatedServer.inGtidMode();
	}

	public static Position capture(Connection c) throws SQLException {
		return Position.capture(c, inGtidMode());
	}

	public List<RowMap> getRowsWithReplicator(final MysqlIsolatedServer mysql, final MaxwellFilter filter, final MaxwellTestSupportCallback callback, final Optional<MaxwellOutputConfig> optionalOutputConfig) throws Exception {
		final ArrayList<RowMap> list = new ArrayList<>();

		clearSchemaStore(mysql);

		BaseMaxwellConfig config = maxwellConfigFactory.createNewDefaultConfiguration();

		config.getMaxwellMysql().setUser("maxwell");
		config.getMaxwellMysql().setPassword("maxwell");
		config.getMaxwellMysql().setHost("localhost");
		config.getMaxwellMysql().setPort(mysql.getPort());
		config.getMaxwellMysql().setSslMode(SSLMode.DISABLED);
		config.setReplicationMysql(config.getMaxwellMysql());
		final MaxwellOutputConfig outputConfig = optionalOutputConfig.orElseGet(BaseMaxwellOutputConfig::new);

		if ( filter != null ) {
			if ( filter.isDatabaseWhitelist() )
				filter.includeDatabase("test");
			if ( filter.isTableWhitelist() )
				filter.includeTable("boundary");
		}

		config.setFilter(filter);
		config.setBootstrapperType("sync");
		config.setProducerType("buffer");

		callback.beforeReplicatorStart(mysql);

		config.setInitPosition(capture(mysql.getConnection()));
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

	public void testDDLFollowing(MysqlIsolatedServer server, String alters[]) throws Exception {
		SchemaCapturer capturer = new SchemaCapturer(server.getConnection(), maxwellConfigTestSupport.buildContext(server.getPort(), null, null).getCaseSensitivity());
		Schema topSchema = capturer.capture();

		server.executeList(Arrays.asList(alters));

		ObjectMapper m = new ObjectMapper();

		for ( String alterSQL : alters) {
			List<SchemaChange> changes = SchemaChange.parse("shard_1", alterSQL);
			if ( changes != null ) {
				for ( SchemaChange change : changes ) {
					ResolvedSchemaChange resolvedChange = change.resolve(topSchema);

					if ( resolvedChange == null )
						continue;

					// go to and from json
					String json = m.writeValueAsString(resolvedChange);
					ResolvedSchemaChange fromJson = m.readValue(json, ResolvedSchemaChange.class);

					fromJson.apply(topSchema);
				}
			}
		}

		Schema bottomSchema = capturer.capture();

		List<String> diff = topSchema.diff(bottomSchema, "followed schema", "recaptured schema");
		assertThat(StringUtils.join(diff.iterator(), "\n"), diff.size(), is(0));
	}

	public RowMap pollRowFromBufferedProducer(MaxwellContext context, long ms) throws IOException, InterruptedException {
		BufferedProducer p = (BufferedProducer) context.getProducer();
		return p.poll(ms, TimeUnit.MILLISECONDS);
	}
}
