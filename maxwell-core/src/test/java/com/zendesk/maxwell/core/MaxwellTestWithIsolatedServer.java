package com.zendesk.maxwell.core;

import com.zendesk.maxwell.core.config.MaxwellFilter;
import com.zendesk.maxwell.core.producer.MaxwellOutputConfig;
import com.zendesk.maxwell.core.replication.MysqlVersion;
import com.zendesk.maxwell.core.replication.Position;
import com.zendesk.maxwell.core.row.RowMap;
import com.zendesk.maxwell.core.util.Logging;
import org.junit.Before;
import org.junit.BeforeClass;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.TimeZone;

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

	@Before
	public void setupSchema() throws Exception {
		MaxwellTestSupport.setupSchema(server);
	}

	protected List<RowMap> getRowsForSQL(MaxwellFilter filter, String[] input) throws Exception {
		return MaxwellTestSupport.getRowsWithReplicator(server, filter, input, null);
	}

	protected List<RowMap> getRowsForSQL(MaxwellFilter filter, String[] input, String[] before) throws Exception {
		return MaxwellTestSupport.getRowsWithReplicator(server, filter, input, before);
	}

	protected List<RowMap> getRowsForSQL(String[] input) throws Exception {
		return MaxwellTestSupport.getRowsWithReplicator(server, null, input, null);
	}

	protected List<RowMap> getRowsForSQLTransactional(final String[] input) throws Exception {
		return getRowsForSQLTransactional(input, null, null);
	}

	protected List<RowMap> getRowsForSQLTransactional(final String[] input, MaxwellFilter filter, MaxwellOutputConfig outputConfig) throws Exception {
		MaxwellTestSupportCallback callback = new MaxwellTestSupportCallback() {
			@Override
			public void afterReplicatorStart(MysqlIsolatedServer mysql) throws SQLException {
				Connection c = mysql.getNewConnection();
				c.setAutoCommit(false);
				for (String s : input) {
					c.createStatement().execute(s);
				}
				c.commit();
			}
		};
		return MaxwellTestSupport.getRowsWithReplicator(server, filter, callback, outputConfig);
	}

	protected List<RowMap> getRowsForDDLTransaction(String[] sql, MaxwellFilter filter) throws Exception {
		MaxwellOutputConfig outputConfig = new MaxwellOutputConfig();
		outputConfig.outputDDL = true;
		return getRowsForSQLTransactional(sql, filter, outputConfig);
	}

	protected void runJSON(String filename) throws Exception {
		MaxwellTestJSON.runJSONTestFile(server, filename, null, null);
	}

	protected void runJSON(String filename, MaxwellFilter filter) throws Exception {
		MaxwellTestJSON.runJSONTestFile(server, filename, filter, null);
	}

	protected void runJSON(String filename, MaxwellOutputConfig outputConfig) throws Exception {
		MaxwellTestJSON.runJSONTestFile(server, filename, null, outputConfig);
	}

	protected MaxwellContext buildContext() throws Exception {
		return MaxwellTestSupport.buildContext(server.getPort(), null, null);
	}

	protected MaxwellContext buildContext(Position p) throws Exception {
		return MaxwellTestSupport.buildContext(server.getPort(), p, null);
	}

	protected MaxwellFilter excludeTable(String name) throws MaxwellInvalidFilterException {
		MaxwellFilter filter = new MaxwellFilter();
		filter.excludeTable(name);
		return filter;
	}

	protected MaxwellFilter excludeDb(String name) throws MaxwellInvalidFilterException {
		MaxwellFilter filter = new MaxwellFilter();
		filter.excludeDatabase(name);
		return filter;
	}

	protected void requireMinimumVersion(MysqlVersion minimum) {
		// skips this test if running an older MYSQL version
		assumeTrue(server.getVersion().atLeast(minimum));
	}
}
