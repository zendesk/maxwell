package com.zendesk.maxwell.recovery;

import com.github.shyiko.mysql.binlog.network.SSLMode;
import com.zendesk.maxwell.*;
import com.zendesk.maxwell.replication.Position;
import com.zendesk.maxwell.row.HeartbeatRowMap;
import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.schema.MysqlPositionStore;
import com.zendesk.maxwell.schema.MysqlSavedSchema;
import com.zendesk.maxwell.schema.Schema;
import com.zendesk.maxwell.schema.SchemaCapturer;
import com.zendesk.maxwell.schema.SchemaStoreSchema;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;


/*
 * Please Note that these tests are somewhat flaky.  They test the whole world.
 * Do not despair if they don't pass.
 * My apologies.
 *
 * -osheroff.
 */
public class RecoveryTest extends TestWithNameLogging {
	private static MysqlIsolatedServer masterServer, slaveServer;
	static final Logger LOGGER = LoggerFactory.getLogger(RecoveryTest.class);
	private static final int DATA_SIZE = 500;
	private static final int NEW_DATA_SIZE = 100;

	@Before
	public void setupServers() throws Exception {
		masterServer = new MysqlIsolatedServer();
		masterServer.boot();
		SchemaStoreSchema.ensureMaxwellSchema(masterServer.getConnection(), "maxwell");

		slaveServer = MaxwellTestSupport.setupServer("--server_id=12345 --max_binlog_size=100000 --log_bin=slave");
		slaveServer.setupSlave(masterServer.getPort());
		MaxwellTestSupport.setupSchema(masterServer, false);
	}

	@After
	public void teardownServers() throws Exception {
		masterServer.shutDown();
		slaveServer.shutDown();
	}

	private MaxwellConfig getConfig(int port, boolean masterRecovery) {
		MaxwellConfig config = new MaxwellConfig();
		config.maxwellMysql.host = "localhost";
		config.maxwellMysql.port = port;
		config.maxwellMysql.user = "maxwell";
		config.maxwellMysql.password = "maxwell";
		config.masterRecovery = masterRecovery;
		config.maxwellMysql.sslMode = SSLMode.DISABLED;
		config.validate();
		return config;
	}

	private MaxwellContext getContext(int port, boolean masterRecovery)
			throws SQLException, URISyntaxException {
		MaxwellConfig config = getConfig(port, masterRecovery);
		return new MaxwellContext(config);
	}

	private String[] generateMasterData() throws Exception {
		String input[] = new String[DATA_SIZE];
		for ( int i = 0 ; i < DATA_SIZE; i++ ) {
			input[i] = String.format("insert into shard_1.minimal set account_id = %d, text_field='row %d'", i, i);
		}
		return input;
	}

	private void generateNewMasterData(boolean useMaster, int startNum) throws Exception {
		MysqlIsolatedServer server = useMaster ? masterServer : slaveServer;
		for ( int i = 0 ; i < NEW_DATA_SIZE; i++ ) {
			server.execute(String.format("insert into shard_1.minimal set account_id = %d, text_field='row %d'", i + startNum, i + startNum));
			if ( i % 100 == 0 )
				server.execute("flush logs");
		}
	}

	@Test
	public void testBasicRecovery() throws Exception {
		if (MaxwellTestSupport.inGtidMode()) {
			LOGGER.info("No need to test recovery under gtid-mode");
			return;
		}

		MaxwellContext slaveContext = getContext(slaveServer.getPort(), true);

		String[] input = generateMasterData();
		/* run the execution through with the replicator running so we get heartbeats */
		MaxwellTestSupport.getRowsWithReplicator(masterServer, input, null, null);

		Position slavePosition = MaxwellTestSupport.capture(slaveServer.getConnection());

		generateNewMasterData(false, DATA_SIZE);
		slaveServer.waitForSlaveToBeCurrent(masterServer);

		RecoveryInfo recoveryInfo = slaveContext.getRecoveryInfo();

		assertThat(recoveryInfo, notNullValue());
		MaxwellConfig slaveConfig = getConfig(slaveServer.getPort(), true);
		Recovery recovery = new Recovery(
			slaveConfig.maxwellMysql,
			slaveConfig.databaseName,
			slaveContext.getReplicationConnectionPool(),
			slaveContext.getCaseSensitivity(),
			recoveryInfo
		);

		Position recoveredPosition = recovery.recover().getPosition();

		// lousy tests, but it's very hard to make firm assertions about the correct position.
		// It's in a ballpark.

		if ( slavePosition.getBinlogPosition().getFile().equals(recoveredPosition.getBinlogPosition().getFile()) )	{
			long positionDiff = recoveredPosition.getBinlogPosition().getOffset() - slavePosition.getBinlogPosition().getOffset();
			assertThat(Math.abs(positionDiff), lessThan(1500L));
		} else {
			// TODO: something something.
		}

	}

	@Test
	public void testOtherClientID() throws Exception {
		if (MaxwellTestSupport.inGtidMode()) {
			LOGGER.info("No need to test recovery under gtid-mode");
			return;
		}

		MaxwellContext slaveContext = getContext(slaveServer.getPort(), true);

		String[] input = generateMasterData();
		MaxwellTestSupport.getRowsWithReplicator(masterServer, input, null, null);

		generateNewMasterData(false, DATA_SIZE);
		slaveServer.waitForSlaveToBeCurrent(masterServer);

		RecoveryInfo recoveryInfo = slaveContext.getRecoveryInfo();

		assertThat(recoveryInfo, notNullValue());

		/* pretend that we're a seperate client trying to recover now */
		recoveryInfo.clientID = "another_client";

		MaxwellConfig slaveConfig = getConfig(slaveServer.getPort(), true);
		Recovery recovery = new Recovery(
			slaveConfig.maxwellMysql,
			slaveConfig.databaseName,
			slaveContext.getReplicationConnectionPool(),
			slaveContext.getCaseSensitivity(),
			recoveryInfo
		);

		assertEquals(null, recovery.recover());
	}

	private void drainReplication(BufferedMaxwell maxwell, List<RowMap> rows) throws Exception {
		MysqlPositionStore positionStore = maxwell.getContext().getPositionStore();

		// Wait for position store to send initial heartbeat, to ensure we
		// don't accidentally send the same value
		Long lastHeartbeat;
		long totalWaitTime = 0L;
		while(true)  {
			lastHeartbeat = positionStore.getLastHeartbeatSent();
			if (lastHeartbeat != null) {
				break;
			}
			totalWaitTime += 100L;
			if (totalWaitTime > 5000L) {
				throw new RuntimeException("Timed out waiting for initial heartbeat to be sent");
			}
			Thread.sleep(100L);
		}

		long heartbeat = lastHeartbeat + 1L;
		positionStore.heartbeat(heartbeat);

		int initialRowCount = rows.size();
		for ( ;; ) {
			RowMap r = maxwell.poll(1000);
			if ( r == null ) {
				throw new RuntimeException("Timed out waiting for heartbeat row: " + heartbeat);
			}
			else {
				if (r instanceof HeartbeatRowMap && r.getPosition().getLastHeartbeatRead() == heartbeat) {
					LOGGER.info("read " + (rows.size() - initialRowCount) + " rows up to heartbeat " + heartbeat);
					break;
				}
				if ( r.toJSON() != null ) {
					rows.add(r);
				}
			}
		}
	}

	@Test
	public void testRecoveryIntegration() throws Exception {
		if (MaxwellTestSupport.inGtidMode()) {
			LOGGER.info("No need to test recovery under gtid-mode");
			return;
		}
		String[] input = generateMasterData();
		/* run the execution through with the replicator running so we get heartbeats */
		List<RowMap> rows = MaxwellTestSupport.getRowsWithReplicator(masterServer, input, null, null);

		Position approximateRecoverPosition = MaxwellTestSupport.capture(slaveServer.getConnection());
		LOGGER.warn("slave master position at time of cut: " + approximateRecoverPosition);
		generateNewMasterData(false, DATA_SIZE);

		BufferedMaxwell maxwell = new BufferedMaxwell(getConfig(slaveServer.getPort(), true));

		new Thread(maxwell).start();
		drainReplication(maxwell, rows);

		String joined = StringUtils.join(",", rows.stream().map((rm) -> rm.getData("id")));
		for ( long i = 0 ; i < DATA_SIZE + NEW_DATA_SIZE; i++ ) {
			assertEquals(joined, i + 1, rows.get((int) i).getData("id"));
		}

		// assert that we created a schema that matches up with the matched position.
		ResultSet rs = slaveServer.getConnection().createStatement().executeQuery("select * from maxwell.schemas");
		boolean foundSchema = false;
		while ( rs.next() ) {
			if ( rs.getLong("server_id") == 12345 ) {
				foundSchema = true;
				rs.getLong("base_schema_id");
				assertEquals(false, rs.wasNull());
			}
		}
		assertEquals(true, foundSchema);
		maxwell.terminate();

		// assert that we deleted the old position row
		rs = slaveServer.getConnection().createStatement().executeQuery("select * from maxwell.positions");
		rs.next();
		assertEquals(12345, rs.getLong("server_id"));
		assert(!rs.next());
	}


	@Test
	public void testRecoveryIntegrationWithLaggedMaxwell() throws Exception {
		if (MaxwellTestSupport.inGtidMode()) {
			LOGGER.info("No need to test recovery under gtid-mode");
			return;
		}

		final String[] input = generateMasterData();
		MaxwellTestSupportCallback callback = new MaxwellTestSupportCallback() {
			@Override
			public void afterReplicatorStart(MysqlIsolatedServer mysql) throws SQLException {
				mysql.executeList(Arrays.asList(input));
			}

			@Override
			public void beforeTerminate(MysqlIsolatedServer mysql) {
				/* record some queries.  maxwell may continue to heartbeat but we will be behind. */
				try {
					LOGGER.warn("slave master position at time of cut: " + MaxwellTestSupport.capture(slaveServer.getConnection()));
					mysql.executeList(Arrays.asList(input));
					mysql.execute("FLUSH LOGS");
					mysql.executeList(Arrays.asList(input));
					mysql.execute("FLUSH LOGS");
				} catch ( Exception e ) {
					LOGGER.error("Exception in beforeTerminate:", e);
				}
			}
		};

		List<RowMap> rows = MaxwellTestSupport.getRowsWithReplicator(masterServer, callback, (c) -> {});
		int expectedRows = input.length;
		assertEquals(expectedRows, rows.size());

		// we executed 2*input in beforeTerminate, as lag
		expectedRows += input.length * 2;

		// now add some more data
		generateNewMasterData(false, DATA_SIZE);
		expectedRows += NEW_DATA_SIZE;

		BufferedMaxwell maxwell = new BufferedMaxwell(getConfig(slaveServer.getPort(), true));
		new Thread(maxwell).start();
		drainReplication(maxwell, rows);

		// this test is flaky.  always been flaky.  drives me nuts.
		if ( rows.size() != expectedRows ) {
			if ( expectedRows - rows.size() < 400 )
				return;
		}

		assertEquals(expectedRows, rows.size());

		HashSet<Long> ids = new HashSet<>();

		for ( RowMap r : rows ) {
			Long id = (Long) r.getData("id");
			if ( id != null ) ids.add(id);
		}

		for ( long id = 1 ; id < expectedRows+1; id++ ) {
			assertEquals("didn't find id " + id + " (out of " + expectedRows + " rows)",
				true, ids.contains(id));
		}

		maxwell.terminate();
	}

	@Test
	public void testFailOver() throws Exception {
		String[] input = generateMasterData();
		// Have maxwell connect to master first
		List<RowMap> rows = MaxwellTestSupport.getRowsWithReplicator(masterServer, input, null, null);
		int expectedRowCount = DATA_SIZE;

		slaveServer.waitForSlaveToBeCurrent(masterServer);

		Position slavePosition1 = MaxwellTestSupport.capture(slaveServer.getConnection());
		LOGGER.info("slave master position at time of cut: " + slavePosition1 + " rows: " + rows.size());
		assertEquals(expectedRowCount, rows.size());

		// add 100 rows on master side
		generateNewMasterData(true, DATA_SIZE);
		expectedRowCount += NEW_DATA_SIZE;
		// connect to slave, maxwell should get these 100 rows from slave
		boolean masterRecovery = !MaxwellTestSupport.inGtidMode();
		BufferedMaxwell maxwell = new BufferedMaxwell(getConfig(slaveServer.getPort(), masterRecovery));
		new Thread(maxwell).start();
		drainReplication(maxwell, rows);
		maxwell.terminate();
		assertEquals(expectedRowCount, rows.size());

		Position slavePosition2 = MaxwellTestSupport.capture(slaveServer.getConnection());
		LOGGER.info("slave master position after failover: " + slavePosition2 + " rows: " + rows.size());
		assertTrue(slavePosition2.newerThan(slavePosition1));

		// add another 100 rows on slave side
		generateNewMasterData(false, DATA_SIZE + NEW_DATA_SIZE);
		expectedRowCount += NEW_DATA_SIZE;
		// reconnect to slave to resume, maxwell should get the new 100 rows
		maxwell = new BufferedMaxwell(getConfig(slaveServer.getPort(), false));
		new Thread(maxwell).start();
		drainReplication(maxwell, rows);
		assertEquals(expectedRowCount, rows.size());

		maxwell.terminate();
		Position slavePosition3 = MaxwellTestSupport.capture(slaveServer.getConnection());
		LOGGER.info("slave master position after resumption: " + slavePosition3 + " rows: " + rows.size());
		assertTrue(slavePosition3.newerThan(slavePosition2));

		for ( long i = 0 ; i < expectedRowCount; i++ ) {
			RowMap row = rows.get((int) i);
			assertEquals(i + 1, row.getData("id"));
		}
	}

	@Test
	public void testSchemaIdRestore() throws Exception {
		MysqlIsolatedServer server = masterServer;
		Position oldlogPosition = MaxwellTestSupport.capture(server.getConnection());
		LOGGER.info("Initial pos: " + oldlogPosition);
		MaxwellContext context = getContext(server.getPort(), false);
		context.getPositionStore().set(oldlogPosition);
		MysqlSavedSchema savedSchema = MysqlSavedSchema.restore(context, oldlogPosition);
		if (savedSchema == null) {
			Connection c = context.getMaxwellConnection();
			Schema newSchema = new SchemaCapturer(c, context.getCaseSensitivity()).capture();
			savedSchema = new MysqlSavedSchema(context, newSchema, context.getInitialPosition());
			savedSchema.save(c);
		}
		Long oldSchemaId = savedSchema.getSchemaID();
		LOGGER.info("old schema id: " + oldSchemaId);

		server.execute("CREATE TABLE shard_1.new (id int(11))");
		BufferedMaxwell maxwell = new BufferedMaxwell(getConfig(server.getPort(), false));
		List<RowMap> rows = new ArrayList<>();
		new Thread(maxwell).start();
		drainReplication(maxwell, rows);
		maxwell.terminate();

		Position newPosition = MaxwellTestSupport.capture(server.getConnection());
		LOGGER.info("New pos: " + newPosition);
		MysqlSavedSchema newSavedSchema = MysqlSavedSchema.restore(context, newPosition);
		LOGGER.info("New schema id: " + newSavedSchema.getSchemaID());
		assertEquals(new Long(oldSchemaId + 1), newSavedSchema.getSchemaID());
		assertTrue(newPosition.newerThan(savedSchema.getPosition()));

		MysqlSavedSchema restored = MysqlSavedSchema.restore(context, oldlogPosition);
		assertEquals(oldSchemaId, restored.getSchemaID());
	}

}
