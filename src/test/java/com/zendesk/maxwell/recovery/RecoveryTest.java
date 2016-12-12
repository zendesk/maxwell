package com.zendesk.maxwell.recovery;

import com.zendesk.maxwell.*;
import com.zendesk.maxwell.replication.BinlogPosition;
import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.schema.SchemaStoreSchema;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class RecoveryTest extends TestWithNameLogging {
	private static MysqlIsolatedServer masterServer, slaveServer;
	static final Logger LOGGER = LoggerFactory.getLogger(RecoveryTest.class);

	@Before
	public void setupServers() throws Exception {
		masterServer = new MysqlIsolatedServer();
		masterServer.boot();
		SchemaStoreSchema.ensureMaxwellSchema(masterServer.getConnection(), "maxwell");

		slaveServer = MaxwellTestSupport.setupServer("--server_id=12345 --max_binlog_size=100000 --log_bin=slave");
		slaveServer.setupSlave(masterServer.getPort());
		MaxwellTestSupport.setupSchema(masterServer, false);
	}

	private MaxwellConfig getConfig(int port) {
		MaxwellConfig config = new MaxwellConfig();
		config.maxwellMysql.host = "localhost";
		config.maxwellMysql.port = port;
		config.maxwellMysql.user = "maxwell";
		config.maxwellMysql.password = "maxwell";
		config.masterRecovery = true;
		config.validate();
		return config;
	}

	private MaxwellContext getContext(int port) throws SQLException {
		MaxwellConfig config = getConfig(port);
		return new MaxwellContext(config);
	}

	private String[] generateMasterData() throws Exception {
		String input[] = new String[5000];
		for ( int i = 0 ; i < 5000; i++ ) {
			input[i] = String.format("insert into shard_1.minimal set account_id = %d, text_field='row %d'", i, i);
		}
		return input;
	}

	private void generateNewMasterData() throws Exception {
		for ( int i = 0 ; i < 1000; i++ ) {
			slaveServer.execute(String.format("insert into shard_1.minimal set account_id = %d, text_field='row %d'", i + 5000, i + 5000));
			if ( i % 100 == 0 )
				slaveServer.execute("flush logs");
		}
	}

	@Test
	public void testBasicRecovery() throws Exception {
		MaxwellContext slaveContext = getContext(slaveServer.getPort());

		String[] input = generateMasterData();
		/* run the execution through with the replicator running so we get heartbeats */
		MaxwellTestSupport.getRowsWithReplicator(masterServer, null, input, null);

		BinlogPosition slavePosition = BinlogPosition.capture(slaveServer.getConnection());

		generateNewMasterData();
		RecoveryInfo recoveryInfo = slaveContext.getRecoveryInfo();

		assertThat(recoveryInfo, notNullValue());
		MaxwellConfig slaveConfig = getConfig(slaveServer.getPort());
		Recovery recovery = new Recovery(
			slaveConfig.maxwellMysql,
			slaveConfig.databaseName,
			slaveContext.getReplicationConnectionPool(),
			slaveContext.getCaseSensitivity(),
			recoveryInfo
		);

		BinlogPosition recoveredPosition = recovery.recover();
		// lousy tests, but it's very hard to make firm assertions about the correct position.
		// It's in a ballpark.

		if ( slavePosition.getFile().equals(recoveredPosition.getFile()) )	{
			long positionDiff = recoveredPosition.getOffset() - slavePosition.getOffset();
			assertThat(Math.abs(positionDiff), lessThan(2000L));
		} else {
			// TODO: something something.
		}

	}

	@Test
	public void testOtherClientID() throws Exception {
		MaxwellContext slaveContext = getContext(slaveServer.getPort());

		String[] input = generateMasterData();
		MaxwellTestSupport.getRowsWithReplicator(masterServer, null, input, null);

		generateNewMasterData();
		RecoveryInfo recoveryInfo = slaveContext.getRecoveryInfo();
		assertThat(recoveryInfo, notNullValue());

		/* pretend that we're a seperate client trying to recover now */
		recoveryInfo.clientID = "another_client";

		MaxwellConfig slaveConfig = getConfig(slaveServer.getPort());
		Recovery recovery = new Recovery(
			slaveConfig.maxwellMysql,
			slaveConfig.databaseName,
			slaveContext.getReplicationConnectionPool(),
			slaveContext.getCaseSensitivity(),
			recoveryInfo
		);

		BinlogPosition recoveredPosition = recovery.recover();
		assertEquals(null, recoveredPosition);
	}

	/* i know.  it's horrible. */
	private void drainReplication(BufferedMaxwell maxwell, List<RowMap> rows) throws IOException, InterruptedException {
		int pollMS = 10000;
		for ( ;; ) {
			RowMap r = maxwell.poll(pollMS);
			if ( r == null )
				break;
			else {
				if ( r.toJSON() != null )
					rows.add(r);

				pollMS = 500; // once we get a row, we timeout quickly.
			}
		}
	}

	@Test
	public void testRecoveryIntegration() throws Exception {
		String[] input = generateMasterData();
		/* run the execution through with the replicator running so we get heartbeats */
		List<RowMap> rows = MaxwellTestSupport.getRowsWithReplicator(masterServer, null, input, null);

		BinlogPosition approximateRecoverPosition = BinlogPosition.capture(slaveServer.getConnection());
		LOGGER.warn("slave master position at time of cut: " + approximateRecoverPosition);
		generateNewMasterData();

		BufferedMaxwell maxwell = new BufferedMaxwell(getConfig(slaveServer.getPort()));

		new Thread(maxwell).start();
		drainReplication(maxwell, rows);

		for ( long i = 0 ; i < 6000; i++ ) {
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
		maxwell.terminate();

		// assert that we deleted the old position row
		rs = slaveServer.getConnection().createStatement().executeQuery("select * from maxwell.positions");
		rs.next();
		assertEquals(12345, rs.getLong("server_id"));
		assert(!rs.next());
	}


	@Test
	public void testRecoveryIntegrationWithLaggedMaxwell() throws Exception {
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
					LOGGER.warn("slave master position at time of cut: " + BinlogPosition.capture(slaveServer.getConnection()));
					mysql.executeList(Arrays.asList(input));
					mysql.execute("FLUSH LOGS");
					mysql.executeList(Arrays.asList(input));
					mysql.execute("FLUSH LOGS");
				} catch ( Exception e ) {}
			}
		};

		List<RowMap> rows = MaxwellTestSupport.getRowsWithReplicator(masterServer, null, callback);

		generateNewMasterData();

		BufferedMaxwell maxwell = new BufferedMaxwell(getConfig(slaveServer.getPort()));
		new Thread(maxwell).start();
		drainReplication(maxwell, rows);

		assertThat(rows.size(), greaterThanOrEqualTo(16000));

		boolean[] ids = new boolean[16001];

		for ( RowMap r : rows ) {
			Long id = (Long) r.getData("id");
			if ( id != null )
				ids[id.intValue()] = true;
		}


		for ( int i = 1 ; i < 16001; i++ )
			assertEquals("didn't find id " + i, true, ids[i]);

		maxwell.terminate();
	}
}
