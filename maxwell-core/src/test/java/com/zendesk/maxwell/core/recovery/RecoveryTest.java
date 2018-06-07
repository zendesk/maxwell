package com.zendesk.maxwell.core.recovery;

import com.github.shyiko.mysql.binlog.network.SSLMode;
import com.zendesk.maxwell.api.config.MaxwellConfig;
import com.zendesk.maxwell.api.replication.Position;
import com.zendesk.maxwell.api.row.RowMap;
import com.zendesk.maxwell.core.*;
import com.zendesk.maxwell.core.config.BaseMaxwellConfig;
import com.zendesk.maxwell.core.config.BaseMaxwellMysqlConfig;
import com.zendesk.maxwell.core.config.MaxwellConfigFactory;
import com.zendesk.maxwell.core.row.HeartbeatRowMap;
import com.zendesk.maxwell.core.schema.*;
import com.zendesk.maxwell.core.support.MaxwellTestSupport;
import com.zendesk.maxwell.core.support.MaxwellTestSupportCallback;
import com.zendesk.maxwell.test.mysql.MysqlIsolatedServer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.*;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = { SpringTestContextConfiguration.class })
public class RecoveryTest extends TestWithNameLogging {
	private static final Logger LOGGER = LoggerFactory.getLogger(RecoveryTest.class);

	private static final int DATA_SIZE = 500;
	private static final int NEW_DATA_SIZE = 100;

	@Autowired
	private MaxwellTestSupport maxwellTestSupport;
	@Autowired
	private MaxwellRunner maxwellRunner;
	@Autowired
	private MaxwellContextFactory maxwellContextFactory;
	@Autowired
	private MaxwellConfigFactory maxwellConfigFactory;

	private MysqlIsolatedServer masterServer;
	private MysqlIsolatedServer slaveServer;

	@Before
	public void setupServers() throws Exception {
		masterServer = new MysqlIsolatedServer();
		masterServer.boot();
		SchemaStoreSchema.ensureMaxwellSchema(masterServer.getConnection(), "maxwell");

		slaveServer = MaxwellTestSupport.setupServer("--server_id=12345 --max_binlog_size=100000 --log_bin=slave");
		slaveServer.setupSlave(masterServer.getPort());
		MaxwellTestSupport.setupSchema(masterServer, false);
	}

	private BaseMaxwellConfig getConfig(int port, boolean masterRecovery){
		BaseMaxwellConfig config = maxwellConfigFactory.createNewDefaultConfiguration();
		((BaseMaxwellMysqlConfig)config.getMaxwellMysql()).setHost("localhost");
		((BaseMaxwellMysqlConfig)config.getMaxwellMysql()).setPort(port);
		((BaseMaxwellMysqlConfig)config.getMaxwellMysql()).setUser("maxwell");
		((BaseMaxwellMysqlConfig)config.getMaxwellMysql()).setPassword("maxwell");
		((BaseMaxwellMysqlConfig)config.getMaxwellMysql()).setSslMode(SSLMode.DISABLED);
		config.setMasterRecovery(masterRecovery);
		config.validate();
		return config;
	}

	private MaxwellConfig getBufferedConfig(int port, boolean masterRecovery) {
		BaseMaxwellConfig config = getConfig(port, masterRecovery);
		config.setProducerType("buffer");
		return config;
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
		if (MysqlIsolatedServer.inGtidMode()) {
			LOGGER.info("No need to test recovery under gtid-mode");
			return;
		}

		MaxwellSystemContext slaveContext = maxwellContextFactory.createFor(getBufferedConfig(slaveServer.getPort(), true));

		String[] input = generateMasterData();
		/* run the execution through with the replicator running so we get heartbeats */
		maxwellTestSupport.getRowsWithReplicator(masterServer, null, input, null);

		Position slavePosition = MaxwellTestSupport.capture(slaveServer.getConnection());

		generateNewMasterData(false, DATA_SIZE);
		RecoveryInfo recoveryInfo = slaveContext.getRecoveryInfo();

		assertThat(recoveryInfo, notNullValue());
		MaxwellConfig slaveConfig = getBufferedConfig(slaveServer.getPort(), true);
		Recovery recovery = new Recovery(
				slaveConfig.getMaxwellMysql(),
				slaveConfig.getDatabaseName(),
			slaveContext.getReplicationConnectionPool(),
			slaveContext.getCaseSensitivity(),
			recoveryInfo
		);

		Position recoveredPosition = recovery.recover();
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
		if (MysqlIsolatedServer.inGtidMode()) {
			LOGGER.info("No need to test recovery under gtid-mode");
			return;
		}

		MaxwellSystemContext slaveContext = maxwellContextFactory.createFor(getBufferedConfig(slaveServer.getPort(), true));

		String[] input = generateMasterData();
		maxwellTestSupport.getRowsWithReplicator(masterServer, null, input, null);

		generateNewMasterData(false, DATA_SIZE);
		RecoveryInfo recoveryInfo = slaveContext.getRecoveryInfo();
		assertThat(recoveryInfo, notNullValue());

		/* pretend that we're a seperate client trying to recover now */
		recoveryInfo.clientID = "another_client";

		MaxwellConfig slaveConfig = getBufferedConfig(slaveServer.getPort(), true);
		Recovery recovery = new Recovery(
				slaveConfig.getMaxwellMysql(),
				slaveConfig.getDatabaseName(),
			slaveContext.getReplicationConnectionPool(),
			slaveContext.getCaseSensitivity(),
			recoveryInfo
		);

		Position recoveredPosition = recovery.recover();
		assertEquals(null, recoveredPosition);
	}

	private void drainReplication(final MaxwellSystemContext context, List<RowMap> rows) throws Exception {
		MysqlPositionStore positionStore = context.getPositionStore();

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
			RowMap r = maxwellTestSupport.pollRowFromBufferedProducer(context,1000);
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
		if (MysqlIsolatedServer.inGtidMode()) {
			LOGGER.info("No need to test recovery under gtid-mode");
			return;
		}
		String[] input = generateMasterData();
		/* run the execution through with the replicator running so we get heartbeats */
		List<RowMap> rows = maxwellTestSupport.getRowsWithReplicator(masterServer, null, input, null);

		Position approximateRecoverPosition = MaxwellTestSupport.capture(slaveServer.getConnection());
		LOGGER.warn("slave master position at time of cut: " + approximateRecoverPosition);
		generateNewMasterData(false, DATA_SIZE);

		final MaxwellSystemContext context = maxwellContextFactory.createFor(getBufferedConfig(slaveServer.getPort(), true));
		new Thread(() -> maxwellRunner.run(context)).start();
		drainReplication(context, rows);

		for ( long i = 0 ; i < DATA_SIZE + NEW_DATA_SIZE; i++ ) {
			assertEquals(i + 1, rows.get((int) i).getData("id"));
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
		maxwellRunner.terminate(context);

		// assert that we deleted the old position row
		rs = slaveServer.getConnection().createStatement().executeQuery("select * from maxwell.positions");
		rs.next();
		assertEquals(12345, rs.getLong("server_id"));
		assert(!rs.next());
	}


	@Test
	public void testRecoveryIntegrationWithLaggedMaxwell() throws Exception {
		if (MysqlIsolatedServer.inGtidMode()) {
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

		List<RowMap> rows = maxwellTestSupport.getRowsWithReplicator(masterServer, null, callback, Optional.empty());
		int expectedRows = input.length;
		assertEquals(expectedRows, rows.size());

		// we executed 2*input in beforeTerminate, as lag
		expectedRows += input.length * 2;

		// now add some more data
		generateNewMasterData(false, DATA_SIZE);
		expectedRows += NEW_DATA_SIZE;

		MaxwellSystemContext context = maxwellContextFactory.createFor(getBufferedConfig(slaveServer.getPort(), true));
		new Thread(() -> maxwellRunner.run(context)).start();
		drainReplication(context, rows);
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

		maxwellRunner.terminate(context);
	}

	@Test
	public void testFailOver() throws Exception {
		String[] input = generateMasterData();
		// Have maxwell connect to master first
		List<RowMap> rows = maxwellTestSupport.getRowsWithReplicator(masterServer, null, input, null);
		int expectedRowCount = DATA_SIZE;
		try {
			// sleep a bit for slave to catch up
			Thread.sleep(1000);
		} catch (InterruptedException ex) {
			LOGGER.info("Got ex: " + ex);
		}

		Position slavePosition1 = MaxwellTestSupport.capture(slaveServer.getConnection());
		LOGGER.info("slave master position at time of cut: " + slavePosition1 + " rows: " + rows.size());
		assertEquals(expectedRowCount, rows.size());

		// add 100 rows on master side
		generateNewMasterData(true, DATA_SIZE);
		expectedRowCount += NEW_DATA_SIZE;
		// connect to slave, maxwell should get these 100 rows from slave
		boolean masterRecovery = !MysqlIsolatedServer.inGtidMode();

		final MaxwellSystemContext context1 = maxwellContextFactory.createFor(getBufferedConfig(slaveServer.getPort(), masterRecovery));
		new Thread(() -> maxwellRunner.run(context1)).start();
		drainReplication(context1, rows);
		maxwellRunner.terminate(context1);
		assertEquals(expectedRowCount, rows.size());

		Position slavePosition2 = MaxwellTestSupport.capture(slaveServer.getConnection());
		LOGGER.info("slave master position after failover: " + slavePosition2 + " rows: " + rows.size());
		assertTrue(slavePosition2.newerThan(slavePosition1));

		// add another 100 rows on slave side
		generateNewMasterData(false, DATA_SIZE + NEW_DATA_SIZE);
		expectedRowCount += NEW_DATA_SIZE;
		// reconnect to slave to resume, maxwell should get the new 100 rows

		final MaxwellSystemContext context2 = maxwellContextFactory.createFor(getBufferedConfig(slaveServer.getPort(), false));
		new Thread(() -> maxwellRunner.run(context2)).start();
		drainReplication(context2, rows);
		assertEquals(expectedRowCount, rows.size());

		maxwellRunner.terminate(context2);
		Position slavePosition3 = maxwellTestSupport.capture(slaveServer.getConnection());
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
		MaxwellSystemContext context1 = maxwellContextFactory.createFor(getConfig(server.getPort(), false));
		context1.getPositionStore().set(oldlogPosition);
		MysqlSavedSchema savedSchema = MysqlSavedSchema.restore(context1, oldlogPosition);
		if (savedSchema == null) {
			Connection c = context1.getMaxwellConnection();
			Schema newSchema = new SchemaCapturer(c, context1.getCaseSensitivity()).capture();
			savedSchema = SavedSchemaSupport.getSavedSchema(context1, newSchema, context1.getInitialPosition());
			savedSchema.save(c);
		}
		Long oldSchemaId = savedSchema.getSchemaID();
		LOGGER.info("old schema id: " + oldSchemaId);

		server.execute("CREATE TABLE shard_1.new (id int(11))");

		final MaxwellSystemContext context2 = maxwellContextFactory.createFor(getBufferedConfig(server.getPort(), false));
		List<RowMap> rows = new ArrayList<>();
		new Thread(() -> maxwellRunner.run(context2)).start();
		drainReplication(context2, rows);
		maxwellRunner.terminate(context2);

		Position newPosition = MaxwellTestSupport.capture(server.getConnection());
		LOGGER.info("New pos: " + newPosition);
		MysqlSavedSchema newSavedSchema = MysqlSavedSchema.restore(context1, newPosition);
		LOGGER.info("New schema id: " + newSavedSchema.getSchemaID());
		assertEquals(new Long(oldSchemaId + 1), newSavedSchema.getSchemaID());
		assertTrue(newPosition.newerThan(savedSchema.getPosition()));

		MysqlSavedSchema restored = MysqlSavedSchema.restore(context1, oldlogPosition);
		assertEquals(oldSchemaId, restored.getSchemaID());
	}

}
