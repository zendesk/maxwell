package com.zendesk.maxwell.core;

import com.zendesk.maxwell.core.bootstrap.AbstractBootstrapper;
import com.zendesk.maxwell.core.config.MaxwellConfig;
import com.zendesk.maxwell.core.producer.AbstractProducer;
import com.zendesk.maxwell.core.recovery.Recovery;
import com.zendesk.maxwell.core.recovery.RecoveryInfo;
import com.zendesk.maxwell.core.replication.BinlogConnectorReplicator;
import com.zendesk.maxwell.core.replication.Position;
import com.zendesk.maxwell.core.replication.Replicator;
import com.zendesk.maxwell.core.schema.MysqlPositionStore;
import com.zendesk.maxwell.core.schema.MysqlSchemaStore;
import com.zendesk.maxwell.core.schema.SchemaStoreSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.SQLException;

public class MaxwellRunner implements Runnable {
	protected MaxwellConfig config;
	protected MaxwellContext context;
	protected Replicator replicator;

	static final Logger LOGGER = LoggerFactory.getLogger(MaxwellRunner.class);

	public MaxwellRunner(MaxwellConfig config) throws SQLException, URISyntaxException {
		this(new MaxwellContext(config));
	}

	protected MaxwellRunner(MaxwellContext context) throws SQLException, URISyntaxException {
		this.config = context.getConfig();
		this.context = context;
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
		Thread terminationThread = this.context.terminate();
		if (terminationThread != null) {
			try {
				terminationThread.join();
			} catch (InterruptedException e) {
				// ignore
			}
		}
	}

	private Position attemptMasterRecovery() throws Exception {
		Position recoveredPosition = null;
		MysqlPositionStore positionStore = this.context.getPositionStore();
		RecoveryInfo recoveryInfo = positionStore.getRecoveryInfo(config);

		if ( recoveryInfo != null ) {
			Recovery masterRecovery = new Recovery(
				config.replicationMysql,
				config.databaseName,
				this.context.getReplicationConnectionPool(),
				this.context.getCaseSensitivity(),
				recoveryInfo
			);

			recoveredPosition = masterRecovery.recover();

			if (recoveredPosition != null) {
				// load up the schema from the recovery position and chain it into the
				// new server_id
				MysqlSchemaStore oldServerSchemaStore = new MysqlSchemaStore(
					context.getMaxwellConnectionPool(),
					context.getReplicationConnectionPool(),
					context.getSchemaConnectionPool(),
					recoveryInfo.serverID,
					recoveryInfo.position,
					context.getCaseSensitivity(),
					config.filter,
					false
				);

				oldServerSchemaStore.clone(context.getServerID(), recoveredPosition);
			}
		}
		return recoveredPosition;
	}

	protected Position getInitialPosition() throws Exception {
		/* first method:  do we have a stored position for this server? */
		Position initial = this.context.getInitialPosition();

		if (initial == null) {

			/* second method: are we recovering from a master swap? */
			if ( config.masterRecovery )
				initial = attemptMasterRecovery();

			/* third method: is there a previous client_id?
			   if so we have to start at that position or else
			   we could miss schema changes, see https://github.com/zendesk/maxwell/issues/782 */

			if ( initial == null ) {
				initial = this.context.getOtherClientPosition();
				if ( initial != null ) {
					LOGGER.info("Found previous client position: " + initial);
				}
			}

			/* fourth method: capture the current master position. */
			if ( initial == null ) {
				try ( Connection c = context.getReplicationConnection() ) {
					initial = Position.capture(c, config.gtidMode);
				}
			}

			/* if the initial position didn't come from the store, store it */
			context.getPositionStore().set(initial);
		}

		if (config.masterRecovery) {
			this.context.getPositionStore().cleanupOldRecoveryInfos();
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
	private void logBanner(AbstractProducer producer, Position initialPosition) {
		String producerName = producer.getClass().getSimpleName();
		LOGGER.info(String.format(bootString, getMaxwellVersion(), producerName, initialPosition.toString()));
	}

	protected void onReplicatorStart() {}
	protected void onReplicatorEnd() {}

	public void start() throws Exception {
		try {
			startInner();
		} catch ( Exception e) {
			this.context.terminate(e);
		} finally {
			onReplicatorEnd();
			this.terminate();
		}

		Exception error = this.context.getError();
		if (error != null) {
			throw error;
		}
	}

	private void startInner() throws Exception {
		try ( Connection connection = this.context.getReplicationConnection();
		      Connection rawConnection = this.context.getRawMaxwellConnection() ) {
			MaxwellMysqlStatus.ensureReplicationMysqlState(connection);
			MaxwellMysqlStatus.ensureMaxwellMysqlState(rawConnection);
			if (config.gtidMode) {
				MaxwellMysqlStatus.ensureGtidMysqlState(connection);
			}

			SchemaStoreSchema.ensureMaxwellSchema(rawConnection, this.config.databaseName);

			try ( Connection schemaConnection = this.context.getMaxwellConnection() ) {
				SchemaStoreSchema.upgradeSchemaStoreSchema(schemaConnection);
			}
		}

		AbstractProducer producer = this.context.getProducer();
		AbstractBootstrapper bootstrapper = this.context.getBootstrapper();

		Position initPosition = getInitialPosition();
		logBanner(producer, initPosition);
		this.context.setPosition(initPosition);

		MysqlSchemaStore mysqlSchemaStore = new MysqlSchemaStore(this.context, initPosition);
		mysqlSchemaStore.getSchema(); // trigger schema to load / capture before we start the replicator.

		this.replicator = new BinlogConnectorReplicator(mysqlSchemaStore, producer, bootstrapper, this.context, initPosition);

		bootstrapper.resume(producer, replicator);

		replicator.setFilter(context.getFilter());

		context.setReplicator(replicator);
		this.context.start();
		this.onReplicatorStart();

		replicator.runLoop();
	}
}
