package com.zendesk.maxwell;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

import com.zendesk.maxwell.producer.MaxwellOutputConfig;
import com.zendesk.maxwell.replication.BinlogPosition;
import com.zendesk.maxwell.row.RowMap;
import org.junit.*;


public class MaxwellTestWithIsolatedServer extends TestWithNameLogging {
	protected static MysqlIsolatedServer server;
	static {
		TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
	}

	@BeforeClass
	public static void setupTest() throws Exception {
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
		MaxwellTestJSON.runJSONTestFile(server, filename, null);
	}

	protected void runJSON(String filename, MaxwellFilter filter) throws Exception {
		MaxwellTestJSON.runJSONTestFile(server, filename, filter);
	}

	protected MaxwellContext buildContext() throws Exception {
		return MaxwellTestSupport.buildContext(server.getPort(), null, null);
	}

	protected MaxwellContext buildContext(BinlogPosition p) throws Exception {
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
}
