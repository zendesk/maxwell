package com.zendesk.maxwell;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.function.Consumer;

import com.zendesk.maxwell.filtering.Filter;
import com.zendesk.maxwell.filtering.InvalidFilterException;
import com.zendesk.maxwell.producer.MaxwellOutputConfig;
import com.zendesk.maxwell.replication.MysqlVersion;
import com.zendesk.maxwell.replication.Position;
import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.util.Logging;
import org.junit.*;

import static org.junit.Assume.assumeTrue;


public class MaxwellTestWithIsolatedServer extends TestWithNameLogging {
	protected static MysqlIsolatedServer server;
	static {
		TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
	}

	@BeforeClass
	public static void setupTest() throws Exception {
		Logging.setupLogBridging();
		server = MaxwellTestSupport.setupServer();
	}

	@AfterClass
	public static void shutdownServer() throws Exception { 
		server.shutDown();
	}

	@Before
	public void setupSchema() throws Exception {
		MaxwellTestSupport.setupSchema(server);
	}

	protected List<RowMap> getRowsForSQL(Filter filter, String[] input) throws Exception {
		return getRowsForSQL(filter, input, null);
	}

	protected List<RowMap> getRowsForSQL(String[] input) throws Exception {
		return getRowsForSQL(null, input, null);
	}

	protected List<RowMap> getRowsForSQL(Filter filter, String[] input, String[] before) throws Exception {
		return MaxwellTestSupport.getRowsWithReplicator(server, input, before, (config) -> {
			if ( filter != null ) {
				try {
					filter.addRule("include: test.*");
				} catch (InvalidFilterException e) { }
			}

			config.filter = filter;
		});
	}

	final int HUGE_NUM_DBS = 1000;
	final int HUGE_NUM_TABLES = 200;

	protected void generateHugeSchema() throws Exception {
		for ( int i = 0 ; i < HUGE_NUM_DBS; i++ ) {
			String dbName = "huge_test_" + i;
			server.execute("create database " + dbName);
			for ( int j = 0; j < HUGE_NUM_TABLES; j++) {
				server.executeCached("create table " + dbName + ".huge_tbl_" + j + "("
					+ "intcol" + j + " int NOT NULL PRIMARY KEY AUTO_INCREMENT, "
					+ "strcol" + j + " varchar(255), "
					+ "othercol" + j + " text"
					+ ")");

			}
			long nGenerated = (i + 1) * HUGE_NUM_TABLES;
			System.out.println("generated " + nGenerated + " of " + (HUGE_NUM_DBS * HUGE_NUM_TABLES) + " tables");
		}
	}


	private class MaxwellTestSupportTXCallback extends MaxwellTestSupportCallback {
		private final String[] input;

		public MaxwellTestSupportTXCallback(final String[] input) {
			this.input = input;
		}

		@Override
		public void afterReplicatorStart(MysqlIsolatedServer mysql) throws SQLException {
			Connection c = mysql.getNewConnection();
			c.setAutoCommit(false);
			for (String s : input) {
				c.createStatement().execute(s);
			}
			c.commit();
		}
	}
	protected List<RowMap> getRowsForSQLTransactional(final String[] input) throws Exception {
		MaxwellTestSupportTXCallback cb = new MaxwellTestSupportTXCallback(input);
		return MaxwellTestSupport.getRowsWithReplicator(server, cb, null);
	}
    protected List<RowMap> getRowsForDDLTransaction(String[] input, Filter filter) throws Exception {
		MaxwellTestSupportTXCallback cb = new MaxwellTestSupportTXCallback(input);
		return MaxwellTestSupport.getRowsWithReplicator(server, cb, (config) -> {
			config.outputConfig = new MaxwellOutputConfig();
			config.outputConfig.outputDDL = true;
			config.filter = filter;
		});
	}

	protected List<RowMap> runJSON(String filename) throws Exception {
		return MaxwellTestJSON.runJSONTestFile(server, filename, null);
	}

	protected List<RowMap> runJSON(String filename, Consumer<MaxwellConfig> configLambda) throws Exception {
		return MaxwellTestJSON.runJSONTestFile(server, filename, configLambda);
	}

	protected MaxwellContext buildContext() throws Exception {
		return MaxwellTestSupport.buildContext(server.getPort(), null, null);
	}

	protected MaxwellContext buildContext(Position p) throws Exception {
		return MaxwellTestSupport.buildContext(server.getPort(), p, null);
	}

	protected Filter excludeTable(String name) throws InvalidFilterException {
		Filter filter = new Filter("exclude: *." + name);
		return filter;
	}

	protected Filter excludeDb(String name) throws InvalidFilterException {
		Filter filter = new Filter("exclude: " + name + ".*");
		return filter;
	}

	protected void requireMinimumVersion(MysqlVersion minimum) {
		MaxwellTestSupport.requireMinimumVersion(server, minimum);
	}

	protected void requireMinimumVersion(MysqlVersion minimum, boolean isMariaDB) {
		MaxwellTestSupport.requireMinimumVersion(server, minimum);
		assumeTrue(server.getVersion().isMariaDB == isMariaDB);

	}

	protected void requireMinimumVersion(int major, int minor) {
		requireMinimumVersion(new MysqlVersion(major, minor));
	}
}
