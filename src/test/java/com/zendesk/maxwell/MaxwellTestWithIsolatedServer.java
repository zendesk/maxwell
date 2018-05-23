package com.zendesk.maxwell;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

import com.zendesk.maxwell.filtering.FilterV2;
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

	@Before
	public void setupSchema() throws Exception {
		MaxwellTestSupport.setupSchema(server);
	}

	protected List<RowMap> getRowsForSQL(FilterV2 filter, String[] input) throws Exception {
		return MaxwellTestSupport.getRowsWithReplicator(server, filter, input, null);
	}

	protected List<RowMap> getRowsForSQL(FilterV2 filter, String[] input, String[] before) throws Exception {
		return MaxwellTestSupport.getRowsWithReplicator(server, filter, input, before);
	}

	protected List<RowMap> getRowsForSQL(String[] input) throws Exception {
		return MaxwellTestSupport.getRowsWithReplicator(server, null, input, null);
	}

	protected List<RowMap> getRowsForSQLTransactional(final String[] input) throws Exception {
		return getRowsForSQLTransactional(input, null, null);
	}

	protected List<RowMap> getRowsForSQLTransactional(final String[] input, FilterV2 filter, MaxwellOutputConfig outputConfig) throws Exception {
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

	protected List<RowMap> getRowsForDDLTransaction(String[] sql, FilterV2 filter) throws Exception {
		MaxwellOutputConfig outputConfig = new MaxwellOutputConfig();
		outputConfig.outputDDL = true;
		return getRowsForSQLTransactional(sql, filter, outputConfig);
	}

	protected void runJSON(String filename) throws Exception {
		MaxwellTestJSON.runJSONTestFile(server, filename, null, null);
	}

	protected void runJSON(String filename, FilterV2 filter) throws Exception {
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

	protected FilterV2 excludeTable(String name) throws MaxwellInvalidFilterException {
		FilterV2 filter = new FilterV2("exclude: *." + name, "");
		return filter;
	}

	protected FilterV2 excludeDb(String name) throws MaxwellInvalidFilterException {
		FilterV2 filter = new FilterV2("exclude: " + name + ".*", "");
		return filter;
	}

	protected void requireMinimumVersion(MysqlVersion minimum) {
		// skips this test if running an older MYSQL version
		assumeTrue(server.getVersion().atLeast(minimum));
	}
}
