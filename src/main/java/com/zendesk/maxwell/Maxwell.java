package com.zendesk.maxwell;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.djdch.log4j.StaticShutdownCallbackRegistry;
import com.zendesk.maxwell.bootstrap.AbstractBootstrapper;
import com.zendesk.maxwell.metrics.MaxwellMetrics;
import com.zendesk.maxwell.producer.AbstractProducer;
import com.zendesk.maxwell.recovery.Recovery;
import com.zendesk.maxwell.recovery.RecoveryInfo;
import com.zendesk.maxwell.replication.BinlogConnectorReplicator;
import com.zendesk.maxwell.replication.BinlogPosition;
import com.zendesk.maxwell.replication.MaxwellReplicator;
import com.zendesk.maxwell.replication.Replicator;
import com.zendesk.maxwell.schema.MysqlPositionStore;
import com.zendesk.maxwell.schema.MysqlSchemaStore;
import com.zendesk.maxwell.schema.SchemaStoreSchema;
import com.zendesk.maxwell.util.Logging;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;

public class Maxwell implements Runnable {
	static {
		Logging.setupLogBridging();
	}

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
		if (this.context.getError() == null) {
			LOGGER.info("starting shutdown");
			try {
				// send a final heartbeat through the system
				context.heartbeat();
				Thread.sleep(100);
				context.terminate();
			} catch (InterruptedException e) {
			} catch (Exception e) {
				LOGGER.error("failed graceful shutdown", e);
			}
		}
	}

	private BinlogPosition attemptMasterRecovery() throws Exception {
		BinlogPosition recovered = null;
		MysqlPositionStore positionStore = this.context.getPositionStore();
		RecoveryInfo recoveryInfo = positionStore.getRecoveryInfo(config);

		if ( recoveryInfo != null ) {
			Recovery masterRecovery = new Recovery(
				config.replicationMysql,
				config.databaseName,
				this.context.getReplicationConnectionPool(),
				this.context.getCaseSensitivity(),
				recoveryInfo,
				this.config.shykoMode
			);

			recovered = masterRecovery.recover();

			if (recovered != null) {
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
				initial = BinlogPosition.capture(c, config.gtidMode);
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
		MaxwellMetrics.setup(config);
		try {
			startInner();
		} finally {
			this.context.terminate();
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

		BinlogPosition initPosition = getInitialPosition();
		logBanner(producer, initPosition);
		this.context.setPosition(initPosition);

		MysqlSchemaStore mysqlSchemaStore = new MysqlSchemaStore(this.context, initPosition);
		mysqlSchemaStore.getSchema(); // trigger schema to load / capture before we start the replicator.

		if ( this.config.shykoMode )
			this.replicator = new BinlogConnectorReplicator(mysqlSchemaStore, producer, bootstrapper, this.context, initPosition);
		else
			this.replicator = new MaxwellReplicator(mysqlSchemaStore, producer, bootstrapper, this.context, initPosition);

		bootstrapper.resume(producer, replicator);

		replicator.setFilter(context.getFilter());

		this.context.addTask(replicator);
		this.context.start();
		this.onReplicatorStart();

		// Dropwizard throws an exception if you try to register multiple metrics with the same name.
		// Since there are codepaths that create multiple replicators (at least in the tests) we need to protect
		// against that.
		String lagGaugeName = MetricRegistry.name(MaxwellMetrics.getMetricsPrefix(), "replication", "lag");
		if ( !(MaxwellMetrics.metricRegistry.getGauges().containsKey(lagGaugeName)) ) {
			MaxwellMetrics.metricRegistry.register(
					lagGaugeName,
					new Gauge<Long>() {
						@Override
						public Long getValue() {
							return replicator.getReplicationLag();
						}
					}
			);
		}

		replicator.runLoop();
		Exception error = this.context.getError();
		if (error != null) {
			throw error;
		}
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
		} catch ( SQLException e ) {
			// catch SQLException explicitly because we likely don't care about the stacktrace
			LOGGER.error("SQLException: " + e.getLocalizedMessage());
			LOGGER.error(e.getLocalizedMessage());
			System.exit(1);
		} catch ( Exception e ) {
			e.printStackTrace();
			System.exit(1);
		}
	}
}
