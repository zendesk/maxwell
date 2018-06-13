package com.zendesk.maxwell.core;

import com.zendesk.maxwell.core.config.MaxwellConfig;
import com.zendesk.maxwell.core.replication.Position;
import com.zendesk.maxwell.core.bootstrap.Bootstrapper;
import com.zendesk.maxwell.core.bootstrap.BootstrapperFactory;
import com.zendesk.maxwell.core.producer.Producer;
import com.zendesk.maxwell.core.recovery.Recovery;
import com.zendesk.maxwell.core.recovery.RecoveryInfo;
import com.zendesk.maxwell.core.replication.BinlogConnectorReplicator;
import com.zendesk.maxwell.core.replication.Replicator;
import com.zendesk.maxwell.core.schema.MysqlPositionStore;
import com.zendesk.maxwell.core.schema.MysqlSchemaStore;
import com.zendesk.maxwell.core.schema.SchemaStoreSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.util.List;

@Service
public class MaxwellRunner {
	private static final Logger LOGGER = LoggerFactory.getLogger(MaxwellRunner.class);
	private static final String BOOT_STRING = "Maxwell v%s is booting (%s), starting at %s";

	private final BootstrapperFactory bootstrapperFactory;
	private final SchemaStoreSchema schemaStoreSchema;
	private final List<MaxwellTerminationListener> terminationListeners;

	@Autowired
	public MaxwellRunner(BootstrapperFactory bootstrapperFactory, SchemaStoreSchema schemaStoreSchema, List<MaxwellTerminationListener> terminationListeners) {
		this.bootstrapperFactory = bootstrapperFactory;
		this.schemaStoreSchema = schemaStoreSchema;
		this.terminationListeners = terminationListeners;
	}

	public void run(final MaxwellContext context) {
		try {
			start(context);
		} catch (Exception e) {
			LOGGER.error("maxwell encountered an exception", e);
		} finally {
			context.getOnExecutionCompletedEventHandler().ifPresent(c -> c.accept(context));
		}
	}

	public void terminate(final MaxwellContext context) {
		Thread terminationThread = context.terminate();
		if (terminationThread != null) {
			try {
				terminationThread.join();
			} catch (InterruptedException e) {
				// ignore
			}
		}
		terminationListeners.stream().forEach(l -> l.onTermination());
	}

	private Position attemptMasterRecovery(final MaxwellContext context) throws Exception {
		final MysqlPositionStore positionStore = context.getPositionStore();
		final MaxwellConfig config = context.getConfig();
		Position recoveredPosition = null;
		RecoveryInfo recoveryInfo = positionStore.getRecoveryInfo(config);

		if ( recoveryInfo != null ) {
			Recovery masterRecovery = new Recovery(
					config.getReplicationMysql(),
					config.getDatabaseName(),
				context.getReplicationConnectionPool(),
				context.getCaseSensitivity(),
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
						config.getFilter(),
					false
				);

				oldServerSchemaStore.clone(context.getServerID(), recoveredPosition);
			}
		}
		return recoveredPosition;
	}

	private Position getInitialPosition(final MaxwellContext context) throws Exception {
		/* first method:  do we have a stored position for this server? */
		Position initial = context.getInitialPosition();
		MaxwellConfig config = context.getConfig();

		if (initial == null) {

			/* second method: are we recovering from a master swap? */
			if (config.isMasterRecovery())
				initial = attemptMasterRecovery(context);

			/* third method: is there a previous client_id?
			   if so we have to start at that position or else
			   we could miss schema changes, see https://github.com/zendesk/maxwell/issues/782 */

			if ( initial == null ) {
				initial = context.getOtherClientPosition();
				if ( initial != null ) {
					LOGGER.info("Found previous client position: " + initial);
				}
			}

			/* fourth method: capture the current master position. */
			if ( initial == null ) {
				try ( Connection c = context.getReplicationConnection() ) {
					initial = Position.capture(c, config.getGtidMode());
				}
			}

			/* if the initial position didn't come from the store, store it */
			context.getPositionStore().set(initial);
		}

		if (config.isMasterRecovery()) { context.getPositionStore().cleanupOldRecoveryInfos();
		}

		return initial;
	}

	private String getMaxwellVersion() {
		String packageVersion = getClass().getPackage().getImplementationVersion();
		if ( packageVersion == null )
			return "??";
		else
			return packageVersion;
	}

	private void logBanner(Producer producer, Position initialPosition) {
		String producerName = producer.getClass().getSimpleName();
		LOGGER.info(String.format(BOOT_STRING, getMaxwellVersion(), producerName, initialPosition.toString()));
	}

	public void start(final MaxwellContext context) throws Exception {
		try {
			context.probeConnections();
			startInner(context);
		} catch ( Exception e) {
			context.terminate(e);
		} finally {
			onReplicatorEnd(context);
			this.terminate(context);
		}

		Exception error = context.getError();
		if (error != null) {
			throw error;
		}
	}

	private void startInner(final MaxwellContext context) throws Exception {
		final MaxwellConfig config = context.getConfig();
		try ( Connection connection = context.getReplicationConnection();
		      Connection rawConnection = context.getRawMaxwellConnection() ) {
			MaxwellMysqlStatus.ensureReplicationMysqlState(connection);
			MaxwellMysqlStatus.ensureMaxwellMysqlState(rawConnection);
			if (config.getGtidMode()) {
				MaxwellMysqlStatus.ensureGtidMysqlState(connection);
			}

			schemaStoreSchema.ensureMaxwellSchema(rawConnection, config.getDatabaseName());

			try ( Connection schemaConnection = context.getMaxwellConnection() ) {
				schemaStoreSchema.upgradeSchemaStoreSchema(schemaConnection);
			}
		}

		Producer producer = context.getProducer();
		Bootstrapper bootstrapper = bootstrapperFactory.createFor(context);

		Position initPosition = getInitialPosition(context);
		logBanner(producer, initPosition);
		context.setPosition(initPosition);

		MysqlSchemaStore mysqlSchemaStore = new MysqlSchemaStore(context, initPosition);
		mysqlSchemaStore.getSchema(); // trigger schema to load / capture before we start the replicator.

		Replicator replicator = new BinlogConnectorReplicator(mysqlSchemaStore, producer, bootstrapper, context, initPosition);

		bootstrapper.resume(producer, replicator);

		replicator.setFilter(context.getFilter());

		context.setReplicator(replicator);
		context.start();

		this.onReplicatorStart(context);

		replicator.runLoop();
	}

	private void onReplicatorStart(MaxwellContext maxwellContext){
		maxwellContext.getOnReplicationStartEventHandler().ifPresent(c -> c.accept(maxwellContext));
	}

	private void onReplicatorEnd(MaxwellContext maxwellContext){
		maxwellContext.getOnReplicationCompletedEventHandler().ifPresent(c -> c.accept(maxwellContext));
	}
}
