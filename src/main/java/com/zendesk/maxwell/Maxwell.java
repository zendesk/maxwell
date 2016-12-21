package com.zendesk.maxwell;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.TimeoutException;
import com.djdch.log4j.StaticShutdownCallbackRegistry;
import com.zendesk.maxwell.replication.BinlogPosition;
import com.zendesk.maxwell.replication.MaxwellReplicator;
import com.zendesk.maxwell.recovery.Recovery;
import com.zendesk.maxwell.recovery.RecoveryInfo;
import com.zendesk.maxwell.replication.Replicator;
import com.zendesk.maxwell.schema.MysqlPositionStore;
import com.zendesk.maxwell.util.Logging;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zendesk.maxwell.bootstrap.AbstractBootstrapper;
import com.zendesk.maxwell.producer.AbstractProducer;
import com.zendesk.maxwell.schema.MysqlSchemaStore;
import com.zendesk.maxwell.schema.SchemaStoreSchema;

public class Maxwell implements Runnable {
	protected MaxwellConfig config;
	protected MaxwellContext context;
	protected Replicator replicator;

	static final Logger LOGGER = LoggerFactory.getLogger(Maxwell.class);

	public Maxwell(MaxwellConfig config) throws SQLException {
		this.config = config;
		this.context = new MaxwellContext(this.config);
		this.context.probeConnections();
	}

	public void run() {
		try {
			start();
		} catch (Exception e) {
			LOGGER.error("maxwell encountered an exception", e);
		}
	}

	public void terminate() {
		LOGGER.info("starting shutdown");
		try {
			// send a final heartbeat through the system
			context.heartbeat();
			Thread.sleep(100);

			if ( this.replicator != null)
				replicator.stopLoop();
		} catch (TimeoutException e) {
			System.err.println("Timed out trying to shutdown maxwell replication thread.");
		} catch (InterruptedException e) {
		} catch (Exception e) { }

		if ( this.context != null )
			context.terminate();

		replicator = null;
		context = null;
	}

	private BinlogPosition attemptMasterRecovery() throws Exception {
		BinlogPosition recovered = null;
		MysqlPositionStore positionStore = this.context.getPositionStore();
		RecoveryInfo recoveryInfo = positionStore.getRecoveryInfo();

		if ( recoveryInfo != null ) {
			Recovery masterRecovery = new Recovery(
				config.replicationMysql,
				config.databaseName,
				this.context.getReplicationConnectionPool(),
				this.context.getCaseSensitivity(),
				recoveryInfo
			);

			recovered = masterRecovery.recover();

			if (recovered != null) {
				// load up the schema from the recovery position and chain it into the
				// new server_id
				MysqlSchemaStore oldServerSchemaStore = new MysqlSchemaStore(
					context.getMaxwellConnectionPool(),
					context.getReplicationConnectionPool(),
					recoveryInfo.serverID,
					recoveryInfo.position,
					context.getCaseSensitivity(),
					config.filter,
					false
				);

				oldServerSchemaStore.clone(context.getServerID(), recovered);

				positionStore.delete(recoveryInfo.serverID, recoveryInfo.clientID, recoveryInfo.position);
			}
		}
		return recovered;
	}

	protected BinlogPosition getInitialPosition() throws Exception {
		/* first method:  do we have a stored position for this server? */
		BinlogPosition initial = this.context.getInitialPosition();

		/* second method: are we recovering from a master swap? */
		if ( initial == null && config.masterRecovery )
			initial = attemptMasterRecovery();

		/* third method: capture the current master postiion. */
		if ( initial == null ) {
			try ( Connection c = context.getReplicationConnection() ) {
				initial = BinlogPosition.capture(c);
			}
		}
		return initial;
	}

	public String getMaxwellVersion() {
		String packageVersion = getClass().getPackage().getImplementationVersion();
		if ( packageVersion == null )
			return "??";
		else
			return packageVersion;
	}

	static String bootString = "Maxwell v%s is booting (%s), starting at %s";
	private void logBanner(AbstractProducer producer, BinlogPosition initialPosition) {
		String producerName = producer.getClass().getSimpleName();
		LOGGER.info(String.format(bootString, getMaxwellVersion(), producerName, initialPosition.toString()));
	}

	protected void onReplicatorStart() {}
	private void start() throws Exception {
		try ( Connection connection = this.context.getReplicationConnection();
			  Connection rawConnection = this.context.getRawMaxwellConnection() ) {
			MaxwellMysqlStatus.ensureReplicationMysqlState(connection);
			MaxwellMysqlStatus.ensureMaxwellMysqlState(rawConnection);

			SchemaStoreSchema.ensureMaxwellSchema(rawConnection, this.config.databaseName);

			try ( Connection schemaConnection = this.context.getMaxwellConnection() ) {
				SchemaStoreSchema.upgradeSchemaStoreSchema(schemaConnection);
			}

		} catch ( SQLException e ) {
			LOGGER.error("SQLException: " + e.getLocalizedMessage());
			LOGGER.error(e.getLocalizedMessage());
			return;
		}

		AbstractProducer producer = this.context.getProducer();
		AbstractBootstrapper bootstrapper = this.context.getBootstrapper();

		BinlogPosition initPosition = getInitialPosition();
		logBanner(producer, initPosition);
		this.context.setPosition(initPosition);

		MysqlSchemaStore mysqlSchemaStore = new MysqlSchemaStore(this.context, initPosition);
		mysqlSchemaStore.getSchema(); // trigger schema to load / capture before we start the replicator.
		this.replicator = new MaxwellReplicator(mysqlSchemaStore, producer, bootstrapper, this.context, initPosition);

		bootstrapper.resume(producer, replicator);

		replicator.setFilter(context.getFilter());

		this.context.start();
		this.onReplicatorStart();
		replicator.runLoop();
	}

	public static void main(String[] args) {
		try {
			MaxwellConfig config = new MaxwellConfig(args);

			if ( config.log_level != null )
				Logging.setLevel(config.log_level);

			final Maxwell maxwell = new Maxwell(config);

			Runtime.getRuntime().addShutdownHook(new Thread() {
				@Override
				public void run() {
					maxwell.terminate();
					StaticShutdownCallbackRegistry.invoke();
				}
			});


			maxwell.start();
		} catch ( Exception e ) {
			e.printStackTrace();
			System.exit(1);
		}
	}
}
