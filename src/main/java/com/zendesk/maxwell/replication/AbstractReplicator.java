package com.zendesk.maxwell.replication;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.zendesk.maxwell.MaxwellFilter;
import com.zendesk.maxwell.bootstrap.AbstractBootstrapper;
import com.zendesk.maxwell.monitoring.Metrics;
import com.zendesk.maxwell.producer.AbstractProducer;
import com.zendesk.maxwell.row.HeartbeatRowMap;
import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.schema.SchemaStore;
import com.zendesk.maxwell.schema.ddl.DDLMap;
import com.zendesk.maxwell.schema.ddl.ResolvedSchemaChange;
import com.zendesk.maxwell.util.RunLoopProcess;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;
import java.util.Objects;

public abstract class AbstractReplicator extends RunLoopProcess implements Replicator {
	private static Logger LOGGER = LoggerFactory.getLogger(AbstractReplicator.class);
	protected final String clientID;
	protected final AbstractProducer producer;
	protected final AbstractBootstrapper bootstrapper;
	protected final String maxwellSchemaDatabaseName;
	protected final TableCache tableCache = new TableCache();
	protected Position lastHeartbeatPosition;
	protected final HeartbeatNotifier heartbeatNotifier;
	protected Long stopAtHeartbeat;
	protected MaxwellFilter filter;

	private final Counter rowCounter;
	private final Meter rowMeter;

	public AbstractReplicator(
		String clientID,
		AbstractBootstrapper bootstrapper,
		String maxwellSchemaDatabaseName,
		AbstractProducer producer,
		Metrics metrics,
		Position initialPosition,
		HeartbeatNotifier heartbeatNotifier
	) {
		this.clientID = clientID;
		this.bootstrapper = bootstrapper;
		this.maxwellSchemaDatabaseName = maxwellSchemaDatabaseName;
		this.producer = producer;
		this.lastHeartbeatPosition = initialPosition;
		this.heartbeatNotifier = heartbeatNotifier;

		rowCounter = metrics.getRegistry().counter(
			metrics.metricName("row", "count")
		);
		rowMeter = metrics.getRegistry().meter(
			metrics.metricName("row", "meter")
		);
	}

	/**
	 * Possibly convert a RowMap object into a HeartbeatRowMap
	 *
	 * Process a rowmap that represents a write to `maxwell`.`heartbeats`.
	 * If it's a write for a different client_id, we return the input (which
	 * will signify to the rest of the chain to ignore it).  Otherwise, we
	 * transform it into a HeartbeatRowMap (which will not be output, but will
	 * advance the binlog position) and set `this.lastHeartbeatPosition`
	 *
	 * @return either a RowMap or a HeartbeatRowMap
	 */
	protected RowMap processHeartbeats(RowMap row) throws SQLException {
		String hbClientID = (String) row.getData("client_id");
		if ( !Objects.equals(hbClientID, this.clientID) )
			return row; // plain row -- do not process.

		long lastHeartbeatRead = (Long) row.getData("heartbeat");
		LOGGER.debug("replicator picked up heartbeat: " + lastHeartbeatRead);
		this.lastHeartbeatPosition = row.getPosition().withHeartbeat(lastHeartbeatRead);
		heartbeatNotifier.heartbeat(lastHeartbeatRead);
		return HeartbeatRowMap.valueOf(row.getDatabase(), this.lastHeartbeatPosition);
	}

	/**
	 * Parse a DDL statement and output the results to the producer
	 *
	 * @param dbName The database "context" under which the SQL is to be processed.  think "use db; alter table foo ..."
	 * @param sql The DDL SQL to be processed
	 * @param schemaStore A SchemaStore object to which we delegate the parsing of the sql
	 * @param position The position that the SQL happened at
	 * @param timestamp The timestamp of the SQL binlog event
	 */
	protected void processQueryEvent(String dbName, String sql, SchemaStore schemaStore, Position position, Long timestamp) throws Exception {
		List<ResolvedSchemaChange> changes = schemaStore.processSQL(sql, dbName, position);
		for (ResolvedSchemaChange change : changes) {
			if (change.shouldOutput(filter)) {
				DDLMap ddl = new DDLMap(change, timestamp, sql, position);
				producer.push(ddl);
			}
		}

		tableCache.clear();
	}

	/**
	 * Should we output an event for the given database and table?
	 *
	 * Here we check against a whitelist/blacklist/filter.  The whitelist
	 * passes updates to `maxwell.bootstrap` through (those are control
	 * mechanisms for bootstrap), the blacklist gets rid of the
	 * `ha_health_check` table which shows up erroneously in Alibaba RDS.
	 *
	 * @param database The database of the DML
	 * @param table The table of the DML
	 * @param filter A table-filter, or null
	 * @return Whether we should write the event to the producer
	 */
	protected boolean shouldOutputEvent(String database, String table, MaxwellFilter filter) {
		Boolean isSystemWhitelisted = this.maxwellSchemaDatabaseName.equals(database)
			&& "bootstrap".equals(table);

		if ( MaxwellFilter.isSystemBlacklisted(database, table) )
			return false;
		else if ( isSystemWhitelisted)
			return true;
		else
			return MaxwellFilter.matches(filter, database, table);
	}


	protected boolean shouldOutputRowMap(String database, String table, RowMap rowMap, MaxwellFilter filter) {
		return MaxwellFilter.matchesValues(filter, database, table, rowMap.getData());
	}

	/**
	 * Get the last heartbeat that the replicator has processed.
	 *
	 * We pass along the value of the heartbeat to the producer inside the row map.
	 * @return the millisecond value ot the last heartbeat read
	 */

	public Long getLastHeartbeatRead() {
		return lastHeartbeatPosition.getLastHeartbeatRead();
	}

	/**
	 * get a single row from the replicator and pass it to the producer or bootstrapper.
	 *
	 * This is the top-level function in the run-loop.
	 */
	public void work() throws Exception {
		RowMap row = getRow();

		if ( row == null )
			return;

		rowCounter.inc();
		rowMeter.mark();

		processRow(row);
	}

	public void stopAtHeartbeat(long heartbeat) {
		stopAtHeartbeat = heartbeat;
	}

	protected void processRow(RowMap row) throws Exception {
		if ( row instanceof HeartbeatRowMap) {
			producer.push(row);
			if (stopAtHeartbeat != null) {
				long thisHeartbeat = row.getPosition().getLastHeartbeatRead();
				if (thisHeartbeat >= stopAtHeartbeat) {
					LOGGER.info("received final heartbeat " + thisHeartbeat + "; stopping replicator");
					// terminate runLoop
					this.taskState.stopped();
				}
			}
		} else if (!bootstrapper.shouldSkip(row) && !isMaxwellRow(row))
			producer.push(row);
		else
			bootstrapper.work(row, producer, this);
	}

	/**
	 * Is this RowMap an update to one of maxwell's own tables?
	 *
	 * If so we will often suppress the output.
	 * @param row The RowMap in question
	 * @return whether the update is something maxwell itself generated
	 */
	protected boolean isMaxwellRow(RowMap row) {
		return row.getDatabase().equals(this.maxwellSchemaDatabaseName);
	}

	/**
	 * The main entry point into the event reading loop.
	 *
	 * We maintain a buffer of events in a transaction,
	 * and each subsequent call to `getRow` can grab one from
	 * the buffer.  If that buffer is empty, we'll go check
	 * the open-replicator buffer for rows to process.  If that
	 * buffer is empty, we return null.
	 *
	 * @return either a RowMap or null
	 */
	public abstract RowMap getRow() throws Exception;

	public void setFilter(MaxwellFilter filter) {
		this.filter = filter;
	}
}
