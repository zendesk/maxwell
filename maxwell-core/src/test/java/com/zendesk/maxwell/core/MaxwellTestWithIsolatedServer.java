package com.zendesk.maxwell.core;

import com.zendesk.maxwell.api.config.MaxwellFilter;
import com.zendesk.maxwell.api.config.MaxwellInvalidFilterException;
import com.zendesk.maxwell.api.config.MaxwellOutputConfig;
import com.zendesk.maxwell.api.replication.Position;
import com.zendesk.maxwell.api.row.RowMap;
import com.zendesk.maxwell.core.config.BaseMaxwellFilter;
import com.zendesk.maxwell.core.config.BaseMaxwellOutputConfig;
import com.zendesk.maxwell.api.replication.MysqlVersion;
import com.zendesk.maxwell.core.support.MaxwellConfigTestSupport;
import com.zendesk.maxwell.core.support.MaxwellTestSupport;
import com.zendesk.maxwell.core.support.MaxwellTestSupportCallback;
import com.zendesk.maxwell.core.util.Logging;
import com.zendesk.maxwell.test.mysql.MysqlIsolatedServer;
import com.zendesk.maxwell.test.mysql.MysqlIsolatedServerSupport;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.TimeZone;

import static org.junit.Assume.assumeTrue;


@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = { SpringTestContextConfiguration.class })
public abstract class MaxwellTestWithIsolatedServer extends TestWithNameLogging {
	static {
		TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
	}

	@Autowired
	protected MaxwellTestSupport maxwellTestSupport;
	@Autowired
	protected MaxwellConfigTestSupport maxwellConfigTestSupport;
	@Autowired
	protected MaxwellTestJSON maxwellTestJSON;
	@Autowired
	protected MysqlIsolatedServerSupport mysqlIsolatedServerSupport;

	protected static MysqlIsolatedServer server;

	@BeforeClass
	public static void setupTest() throws Exception {
		Logging.setupLogBridging();
	}

	@Before
	public void setupDatabase() throws Exception {
		if(server == null){
			server = mysqlIsolatedServerSupport.setupServer();
		}
		mysqlIsolatedServerSupport.setupSchema(server);
	}

	@AfterClass
	public static void cleanupDatabase(){
		if(server != null){
			server.shutDown();
			server = null;
		}
	}

	protected List<RowMap> getRowsForSQL(MaxwellFilter filter, String[] input) throws Exception {
		return maxwellTestSupport.getRowsWithReplicator(server, filter, input, null);
	}

	protected List<RowMap> getRowsForSQL(MaxwellFilter filter, String[] input, String[] before) throws Exception {
		return maxwellTestSupport.getRowsWithReplicator(server, filter, input, before);
	}

	protected List<RowMap> getRowsForSQL(String[] input) throws Exception {
		return maxwellTestSupport.getRowsWithReplicator(server, null, input, null);
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
		return maxwellTestSupport.getRowsWithReplicator(server, filter, callback, Optional.ofNullable(outputConfig));
	}

	protected List<RowMap> getRowsForDDLTransaction(String[] sql, MaxwellFilter filter) throws Exception {
		BaseMaxwellOutputConfig outputConfig = new BaseMaxwellOutputConfig();
		outputConfig.setOutputDDL(true);
		return getRowsForSQLTransactional(sql, filter, outputConfig);
	}

	protected void runJSON(String filename) throws Exception {
		maxwellTestJSON.runJSONTestFile(server, filename, null, null);
	}

	protected void runJSON(String filename, MaxwellFilter filter) throws Exception {
		maxwellTestJSON.runJSONTestFile(server, filename, filter, null);
	}

	protected void runJSON(String filename, MaxwellOutputConfig outputConfig) throws Exception {
		maxwellTestJSON.runJSONTestFile(server, filename, null, outputConfig);
	}

	protected MaxwellSystemContext buildContext() throws Exception {
		return maxwellConfigTestSupport.buildContextWithBufferedProducerFor(server.getPort(), null, null);
	}

	protected MaxwellSystemContext buildContext(Position p) throws Exception {
		return maxwellConfigTestSupport.buildContextWithBufferedProducerFor(server.getPort(), p, null);
	}

	protected MaxwellFilter excludeTable(String name) throws MaxwellInvalidFilterException {
		MaxwellFilter filter = new BaseMaxwellFilter();
		filter.excludeTable(name);
		return filter;
	}

	protected MaxwellFilter excludeDb(String name) throws MaxwellInvalidFilterException {
		MaxwellFilter filter = new BaseMaxwellFilter();
		filter.excludeDatabase(name);
		return filter;
	}

	protected void requireMinimumVersion(MysqlVersion minimum) {
		// skips this test if running an older MYSQL version
		assumeTrue(server.getVersion().atLeast(minimum));
	}
}
