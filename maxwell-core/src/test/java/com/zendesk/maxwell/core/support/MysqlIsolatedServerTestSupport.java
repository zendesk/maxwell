package com.zendesk.maxwell.core.support;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zendesk.maxwell.core.MysqlIsolatedServer;
import com.zendesk.maxwell.core.config.*;
import com.zendesk.maxwell.core.replication.Position;
import com.zendesk.maxwell.core.schema.Schema;
import com.zendesk.maxwell.core.schema.SchemaCapturer;
import com.zendesk.maxwell.core.schema.SchemaStoreSchema;
import com.zendesk.maxwell.core.schema.ddl.ResolvedSchemaChange;
import com.zendesk.maxwell.core.schema.ddl.SchemaChange;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class MysqlIsolatedServerTestSupport {
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


	public static boolean inGtidMode() {
		return System.getenv(MaxwellConfig.GTID_MODE_ENV) != null;
	}

	public static Position capture(Connection c) throws SQLException {
		return Position.capture(c, inGtidMode());
	}

	public static void testDDLFollowing(MysqlIsolatedServer server, String alters[]) throws Exception {
		SchemaCapturer capturer = new SchemaCapturer(server.getConnection(), MaxwellContextTestSupport.buildContext(server.getPort(), null, null).getCaseSensitivity());
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
