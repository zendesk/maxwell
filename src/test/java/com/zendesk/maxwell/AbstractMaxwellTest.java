package com.zendesk.maxwell;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

import com.zendesk.maxwell.schema.SchemaCapturer;
import com.zendesk.maxwell.schema.SchemaStore;

public class AbstractMaxwellTest {
	protected static MysqlIsolatedServer server;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		server = new MysqlIsolatedServer();
		server.boot();
		SchemaStore.createMaxwellSchema(server.getConnection());
	}

	public String getSQLDir() {
		 final String dir = System.getProperty("user.dir");
		 return dir + "/src/test/resources/sql/";
	}


	private void resetMaster() throws SQLException, IOException {
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

	private void generateBinlogEvents() throws IOException, SQLException {
		Path p = Paths.get(getSQLDir() + "/rows/rows.sql");
		List<String>sql = Files.readAllLines(p, Charset.forName("UTF-8"));

		server.executeList(sql);
	}

	@Before
	public void setupMysql() throws SQLException, IOException, InterruptedException {
		resetMaster();
		generateBinlogEvents();
	}

	protected List<MaxwellAbstractRowsEvent>getRowsForSQL(MaxwellFilter filter, String queries[], String before[]) throws Exception {
		BinlogPosition start = BinlogPosition.capture(server.getConnection());
		SchemaCapturer capturer = new SchemaCapturer(server.getConnection());


		if ( before != null ) {
			server.executeList(Arrays.asList(before));
		}

		MaxwellParser p = new MaxwellParser(capturer.capture(), null);
		MaxwellConfig config = new MaxwellConfig();

		config.mysqlHost = "127.0.0.1";
		config.mysqlPort = server.getPort();
		config.mysqlUser = "maxwell";
		config.mysqlPassword = "maxwell";
		config.currentPositionFile = "/tmp/maxwell.position";
		config.setInitialPosition(start);

		p.setConfig(config);

		p.setFilter(filter);

        server.executeList(Arrays.asList(queries));

        p.start();

		ArrayList<MaxwellAbstractRowsEvent> list = new ArrayList<>();
        MaxwellAbstractRowsEvent e;

        while ( (e = p.getEvent()) != null ) {
			if ( !e.getTable().getDatabase().getName().equals("maxwell")) {
				list.add(e);
			}
        }

        p.stop();

        return list;
	}

	protected List<MaxwellAbstractRowsEvent>getRowsForSQL(MaxwellFilter filter, String queries[]) throws Exception {
		return getRowsForSQL(filter, queries, null);
	}

	@After
	public void tearDown() throws Exception {
	}
}
