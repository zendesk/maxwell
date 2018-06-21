package com.zendesk.maxwell;

import com.djdch.log4j.StaticShutdownCallbackRegistry;
import com.zendesk.maxwell.bootstrap.AbstractBootstrapper;
import com.zendesk.maxwell.producer.AbstractProducer;
import com.zendesk.maxwell.recovery.Recovery;
import com.zendesk.maxwell.recovery.RecoveryInfo;
import com.zendesk.maxwell.replication.BinlogConnectorReplicator;
import com.zendesk.maxwell.replication.Position;
import com.zendesk.maxwell.replication.Replicator;
import com.zendesk.maxwell.row.HeartbeatRowMap;
import com.zendesk.maxwell.schema.MysqlPositionStore;
import com.zendesk.maxwell.schema.MysqlSchemaStore;
import com.zendesk.maxwell.schema.SchemaStoreSchema;
import com.zendesk.maxwell.util.Logging;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.SQLException;

public class Maxwell implements Runnable {
	protected MaxwellConfig config;
	protected MaxwellContext context;
	protected Replicator replicator;

	static final Logger LOGGER = LoggerFactory.getLogger(Maxwell.class);

	public Maxwell(MaxwellConfig config) throws SQLException, URISyntaxException {
		this(new MaxwellContext(config));
	}

	protected Maxwell(MaxwellContext context) throws SQLException, URISyntaxException {
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
		HeartbeatRowMap recoveredHeartbeat = null;
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

			recoveredHeartbeat = masterRecovery.recover();

			if (recoveredHeartbeat != null) {
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

				// Note we associate this schema to the start position of the heartbeat event, so that
				// we pick it up when resuming at the event after the heartbeat.
				oldServerSchemaStore.clone(context.getServerID(), recoveredHeartbeat.getPosition());
				return recoveredHeartbeat.getNextPosition();
			}
		}
		return null;
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

	private void start() throws Exception {
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

		if (config.recaptureSchema) {
			mysqlSchemaStore.captureAndSaveSchema();
		}

		mysqlSchemaStore.getSchema(); // trigger schema to load / capture before we start the replicator.

		this.replicator = new BinlogConnectorReplicator(
			mysqlSchemaStore,
			producer,
			bootstrapper,
			config.replicationMysql,
			config.replicaServerID,
			config.databaseName,
			context.getMetrics(),
			initPosition,
			false,
			config.clientID,
			context.getHeartbeatNotifier(),
			config.scripting
		);

		bootstrapper.resume(producer, replicator);

		replicator.setFilter(context.getFilter());

		context.setReplicator(replicator);
		this.context.start();
		this.onReplicatorStart();

		replicator.runLoop();
	}

	public static void main(String[] args) {
		try {
			Logging.setupLogBridging();
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
		} catch ( SQLException e ) {
			// catch SQLException explicitly because we likely don't care about the stacktrace
			LOGGER.error("SQLException: " + e.getLocalizedMessage());
			System.exit(1);
		} catch ( URISyntaxException e ) {
			// catch URISyntaxException explicitly as well to provide more information to the user
			LOGGER.error("Syntax issue with URI, check for misconfigured host, port, database, or JDBC options (see RFC 2396)");
			LOGGER.error("URISyntaxException: " + e.getLocalizedMessage());
			System.exit(1);
		} catch ( Exception e ) {
			e.printStackTrace();
			System.exit(1);
		}
	}
}
