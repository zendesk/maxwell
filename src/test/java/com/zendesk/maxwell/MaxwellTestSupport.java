package com.zendesk.maxwell;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zendesk.maxwell.bootstrap.AsynchronousBootstrapper;
import com.zendesk.maxwell.bootstrap.SynchronousBootstrapper;
import com.zendesk.maxwell.producer.AbstractProducer;
import com.zendesk.maxwell.schema.Schema;
import com.zendesk.maxwell.schema.SchemaCapturer;
import com.zendesk.maxwell.schema.SchemaStore;
import com.zendesk.maxwell.schema.SchemaStoreSchema;
import com.zendesk.maxwell.schema.ddl.ResolvedSchemaChange;
import com.zendesk.maxwell.schema.ddl.SchemaChange;
import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class MaxwellTestSupport {
	public static MysqlIsolatedServer setupServer(String extraParams) throws Exception {
		MysqlIsolatedServer server = new MysqlIsolatedServer();
		server.boot(extraParams);

		SchemaStoreSchema.ensureMaxwellSchema(server.getConnection(), "maxwell");
		return server;
	}

	public static MysqlIsolatedServer setupServer() throws Exception {
		return setupServer(null);
	}

	public static void setupSchema(MysqlIsolatedServer server) throws Exception {
		List<String> queries = new ArrayList<String>(Arrays.asList(
				"CREATE DATABASE if not exists shard_2",
				"DROP DATABASE if exists shard_1",
				"CREATE DATABASE shard_1",
				"USE shard_1"
		));

		for ( File file: new File(getSQLDir() + "/schema").listFiles()) {
			if ( !file.getName().endsWith(".sql"))
				continue;

			byte[] sql = Files.readAllBytes(file.toPath());
			String s = new String(sql);
			if ( s != null ) {
				queries.add(s);
			}
		}

		queries.add("RESET MASTER");

		server.executeList(queries);
	}


	public static String getSQLDir() {
		 final String dir = System.getProperty("user.dir");
		 return dir + "/src/test/resources/sql/";
	}


	public static MaxwellContext buildContext(int port, BinlogPosition p) {
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

		config.initPosition = p;

		return new MaxwellContext(config);
	}

	public static List<RowMap>getRowsForSQL(final MysqlIsolatedServer mysql, MaxwellFilter filter, String queries[], String before[]) throws Exception {
		MaxwellContext context = buildContext(mysql.getPort(), null);

		SchemaCapturer capturer = new SchemaCapturer(mysql.getConnection(), context.getCaseSensitivity());

		if ( before != null ) {
			mysql.executeList(Arrays.asList(before));
		}

		BinlogPosition start = BinlogPosition.capture(mysql.getConnection());

		Schema initialSchema = capturer.capture();
		SchemaStore initialSchemaStore = new SchemaStore(context, initialSchema, BinlogPosition.capture(mysql.getConnection()));
		initialSchemaStore.save(context.getMaxwellConnection());

		mysql.executeList(Arrays.asList(queries));

		BinlogPosition endPosition = BinlogPosition.capture(mysql.getConnection());

		final ArrayList<RowMap> list = new ArrayList<>();

		AbstractProducer producer = new AbstractProducer(context) {
			@Override
			public void push(RowMap r) {
				list.add(r);
			}
		};

		AsynchronousBootstrapper bootstrapper = new AsynchronousBootstrapper(context) {
			@Override
			protected SynchronousBootstrapper getSynchronousBootstrapper() {
				return new SynchronousBootstrapper(context) {
					@Override
					protected Connection getConnection() throws SQLException {
						Connection conn = mysql.getNewConnection();
						conn.setCatalog(context.getConfig().databaseName);
						return conn;
					}

					@Override
					protected Connection getStreamingConnection() throws SQLException {
						Connection conn = mysql.getNewConnection();
						conn.setCatalog(context.getConfig().databaseName);
						return conn;
					}
				};
			}
		};

		TestMaxwellReplicator p = new TestMaxwellReplicator(initialSchemaStore, producer, bootstrapper, context, start, endPosition);

		p.setFilter(filter);

		p.getEvents(producer);

		bootstrapper.join();

		p.stopLoop();

		context.terminate();

		return list;
	}

	public static void testDDLFollowing(MysqlIsolatedServer server, String alters[]) throws Exception {
		SchemaCapturer capturer = new SchemaCapturer(server.getConnection(), buildContext(server.getPort(), null).getCaseSensitivity());
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



	public static  List<RowMap>getRowsForSQL(MysqlIsolatedServer server, MaxwellFilter filter, String queries[]) throws Exception {
		return getRowsForSQL(server, filter, queries, null);
	}
}
