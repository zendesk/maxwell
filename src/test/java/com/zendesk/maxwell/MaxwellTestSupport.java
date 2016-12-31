package com.zendesk.maxwell;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zendesk.maxwell.replication.BinlogPosition;
import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.schema.Schema;
import com.zendesk.maxwell.schema.SchemaCapturer;
import com.zendesk.maxwell.schema.SchemaStoreSchema;
import com.zendesk.maxwell.schema.ddl.ResolvedSchemaChange;
import com.zendesk.maxwell.schema.ddl.SchemaChange;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.nio.file.Files;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class MaxwellTestSupport {
	static final Logger LOGGER = LoggerFactory.getLogger(MaxwellTestSupport.class);

	public static MysqlIsolatedServer setupServer(String extraParams) throws Exception {
		MysqlIsolatedServer server = new MysqlIsolatedServer();
		server.boot(extraParams);

		SchemaStoreSchema.ensureMaxwellSchema(server.getConnection(), "maxwell");
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

		for ( File file: new File(getSQLDir() + "/schema").listFiles()) {
			if ( !file.getName().endsWith(".sql"))
				continue;

			if ( file.getName().contains("sharded") )
				continue;

			byte[] sql = Files.readAllBytes(file.toPath());
			String s = new String(sql);
			if ( s != null ) {
				queries.add(s);
			}
		}

		String shardedFileName;
		if ( server.getVersion().equals("5.6") )
			shardedFileName = "sharded_56.sql";
		else
			shardedFileName = "sharded.sql";

		File shardedFile = new File(getSQLDir() + "/schema/" + shardedFileName);
		byte[] sql = Files.readAllBytes(shardedFile.toPath());
		String s = new String(sql);
		if ( s != null ) {
			queries.add(s);
		}

		if ( resetBinlogs )
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


	public static MaxwellContext buildContext(int port, BinlogPosition p, MaxwellFilter filter) throws SQLException {
		MaxwellConfig config = new MaxwellConfig();

		config.replicationMysql.host = "127.0.0.1";
		config.replicationMysql.port = port;
		config.replicationMysql.user = "maxwell";
		config.replicationMysql.password = "maxwell";

		config.maxwellMysql.host = "127.0.0.1";
		config.maxwellMysql.port = port;
		config.maxwellMysql.user = "maxwell";
		config.maxwellMysql.password = "maxwell";

		config.databaseName = "maxwell";

		config.filter = filter;
		config.initPosition = p;

		return new MaxwellContext(config);
	}

	private static void clearSchemaStore(MysqlIsolatedServer mysql) throws Exception {
		mysql.execute("drop database if exists maxwell");
	}

	public static List<RowMap>getRowsWithReplicator(final MysqlIsolatedServer mysql, MaxwellFilter filter, final String queries[], final String before[]) throws Exception {
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

		return getRowsWithReplicator(mysql, filter, callback);
	}

	public static List<RowMap>getRowsWithReplicator(final MysqlIsolatedServer mysql, MaxwellFilter filter, MaxwellTestSupportCallback callback) throws Exception {
		final ArrayList<RowMap> list = new ArrayList<>();

		clearSchemaStore(mysql);

		MaxwellConfig config = new MaxwellConfig();

		config.maxwellMysql.user = "maxwell";
		config.maxwellMysql.password = "maxwell";
		config.maxwellMysql.host = "localhost";
		config.maxwellMysql.port = mysql.getPort();
		config.replicationMysql = config.maxwellMysql;

		if ( filter != null ) {
			if ( filter.isDatabaseWhitelist() )
				filter.includeDatabase("test");
			if ( filter.isTableWhitelist() )
				filter.includeTable("boundary");
		}

		config.filter = filter;
		config.bootstrapperType = "sync";

		callback.beforeReplicatorStart(mysql);

		config.initPosition = BinlogPosition.capture(mysql.getConnection());
		final String waitObject = new String("");
		BufferedMaxwell maxwell = new BufferedMaxwell(config) {
			@Override
			protected void onReplicatorStart() {
				synchronized(waitObject) {
					waitObject.notify();
				}
			}
		};

		new Thread(maxwell).start();

		synchronized(waitObject) { waitObject.wait(); }

		callback.afterReplicatorStart(mysql);
		maxwell.context.getPositionStore().heartbeat();

		BinlogPosition finalPosition = BinlogPosition.capture(mysql.getConnection());
		LOGGER.debug("running replicator up to " + finalPosition);

		Long pollTime = 1000L;
		BinlogPosition lastPositionRead = null;

		for ( ;; ) {
			RowMap row = maxwell.poll(pollTime);
			pollTime = 500L; // after the first row is receive, we go into a tight loop.

			if ( row == null ) {
				LOGGER.debug("timed out waiting for final row.  Last position we saw: " + lastPositionRead);
				break;
			}

			lastPositionRead = row.getPosition();

			if ( row.getPosition().newerThan(finalPosition) ) {
				// consume whatever's left over in the buffer.
				for ( ;; ) {
					RowMap r = maxwell.poll(100);
					if ( r == null )
						break;

					if ( r.toJSON() != null )
						list.add(r);
				}

				break;
			}
			if ( row.toJSON() != null )
				list.add(row);
		}

		callback.beforeTerminate(mysql);
		maxwell.terminate();

		return list;
	}

	public static void testDDLFollowing(MysqlIsolatedServer server, String alters[]) throws Exception {
		SchemaCapturer capturer = new SchemaCapturer(server.getConnection(), buildContext(server.getPort(), null, null).getCaseSensitivity());
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
}
