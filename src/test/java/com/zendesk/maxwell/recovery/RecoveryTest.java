package com.zendesk.maxwell.recovery;

import com.zendesk.maxwell.*;
import com.zendesk.maxwell.schema.MysqlSchemaStore;
import com.zendesk.maxwell.schema.SchemaStoreSchema;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.SQLException;

import static org.hamcrest.CoreMatchers.is;

public class RecoveryTest {
	private static MysqlIsolatedServer masterServer, slaveServer;

	@BeforeClass
	public static void setupSlaveServer() throws Exception {
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
		return config;
	}

	private MaxwellContext getContext(int port) throws SQLException {
		MaxwellConfig config = getConfig(port);
		config.validate();
		return new MaxwellContext(config);
	}

	private void generateMasterData() throws Exception {
		String input[] = new String[5000];
		for ( int i = 0 ; i < 5000; i++ ) {
			input[i] = String.format("insert into shard_1.minimal set account_id = %d, text_field='row %d'", i, i);
		}
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

		generateMasterData();

		MaxwellTestSupport.getRowsWithReplicator(masterServer, null, input, null);

		BinlogPosition masterPosition = BinlogPosition.capture(masterServer.getConnection());
		BinlogPosition slavePosition = BinlogPosition.capture(slaveServer.getConnection());

		System.out.println("master: " + masterPosition + " slave: " + slavePosition);

		RecoveryInfo recoveryInfo = slaveContext.getRecoveryInfo();

		MaxwellConfig slaveConfig = getConfig(slaveServer.getPort());
		Recovery recovery = new Recovery(
			slaveConfig.maxwellMysql,
			slaveConfig.databaseName,
			new MysqlSchemaStore(slaveContext, null),
			slaveContext.getReplicationConnectionPool(),
			recoveryInfo
		);

		BinlogPosition recoveredPosition = recovery.recover();
		Assert.assertEquals(slavePosition, recoveredPosition);
	}

}
