package com.zendesk.maxwell.replication;

import com.github.shyiko.mysql.binlog.event.*;
import com.zendesk.maxwell.filtering.Filter;
import com.zendesk.maxwell.producer.AbstractProducer;
import com.zendesk.maxwell.producer.MaxwellOutputConfig;
import com.zendesk.maxwell.row.HeartbeatRowMap;
import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.row.RowMapBuffer;
import com.zendesk.maxwell.schema.SchemaStore;
import com.zendesk.maxwell.schema.SchemaStoreException;
import com.zendesk.maxwell.schema.Table;
import com.zendesk.maxwell.schema.columndef.ColumnDefCastException;
import com.zendesk.maxwell.schema.ddl.DDLMap;
import com.zendesk.maxwell.schema.ddl.InvalidSchemaError;
import com.zendesk.maxwell.scripting.Scripting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * @author udyr@shlaji.com
 */
public class BinlogConnectorEventProcessor {
	private static final Logger LOGGER = LoggerFactory.getLogger(BinlogConnectorEventProcessor.class);

	public static final Pattern CREATE_TABLE_PATTERN = Pattern.compile("^CREATE\\s+TABLE", Pattern.CASE_INSENSITIVE);

	private final TableCache tableCache;
	private final MaxwellOutputConfig outputConfig;
	private final Filter filter;
	private final SchemaStore schemaStore;
	private final Scripting scripting;
	private final HeartbeatNotifier heartbeatNotifier;

	private Position lastHeartbeatPosition;

	public BinlogConnectorEventProcessor(TableCache tableCache, SchemaStore schemaStore, Position start, MaxwellOutputConfig outputConfig, Filter filter, Scripting scripting, HeartbeatNotifier heartbeatNotifier) {
		this.tableCache = tableCache;
		this.outputConfig = outputConfig;
		this.filter = filter;
		this.lastHeartbeatPosition = start;
		this.scripting = scripting;
		this.schemaStore = schemaStore;
		this.heartbeatNotifier = heartbeatNotifier;
	}

	public Long getSchemaId() throws SchemaStoreException {
		return this.schemaStore.getSchemaID();
	}

	public BinlogConnectorEvent wrapEvent(Event event, String filename) {
		if (event == null) {
			return null;
		}
		String gtid = null;
		if (event.getHeader().getEventType() == EventType.GTID) {
			gtid = ((GtidEventData) event.getData()).getGtid();
		}
		return new BinlogConnectorEvent(event, filename, null, gtid, outputConfig);
	}

	/**
	 * Writes an event's data to the RowBuffer
	 *
	 * @param event binlog event
	 * @param query sql
	 * @throws ColumnDefCastException event conversion RowMap exception
	 */
	public void writeRow(BinlogConnectorEvent event, RowMapBuffer rowBuffer, String query) throws ColumnDefCastException {
		Table table = tableCache.getTable(event.getTableID());
		if (table == null || !shouldOutputEvent(table.getDatabase(), table.getName(), table.getColumnNames())) {
			return;
		}

		try {
			List<RowMap> rows = event.jsonMaps(table, getLastHeartbeatRead(), query);
			rows.stream().filter(this::shouldOutputRowMap).forEach(r -> {
				try {
					rowBuffer.add(r);
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			});
		} catch (ColumnDefCastException e) {
			logColumnDefCastException(table, e);
			throw e;
		}
	}

	private void logColumnDefCastException(Table table, ColumnDefCastException e) {
		String castInfo = String.format(
				"Unable to cast %s (%s) into column %s.%s.%s (type '%s')",
				e.givenValue.toString(),
				e.givenValue.getClass().getName(),
				table.getDatabase(),
				table.getName(),
				e.def.getName(),
				e.def.getType()
		);
		LOGGER.error(castInfo);

		e.database = table.getDatabase();
		e.table = table.getName();
	}

	private Long getLastHeartbeatRead() {
		return lastHeartbeatPosition == null ? System.currentTimeMillis() : lastHeartbeatPosition.getLastHeartbeatRead();
	}

	public void cacheTable(TableMapEventData data) throws SchemaStoreException {
		if (!shouldOutputEvent(data.getDatabase(), data.getTable(), null)) {
			// There is no need to cache table
			return;
		}
		tableCache.processEvent(schemaStore.getSchema(), filter, data.getTableId(), data.getDatabase(), data.getTable());
	}

	public void clearTableCache() {
		tableCache.clear();
	}

	private boolean shouldOutputRowMap(RowMap row) {
		String database = row.getDatabase();
		String table = row.getTable();
		return filter.isSystemAllowlist(database, table) || filter.includes(database, table, row.getData());
	}

	/**
	 * Should we output a batch of rows for the given database and table?
	 * <p>
	 * First against a allowlist/blocklist/filter.  The allowlist
	 * ensures events that maxwell needs (maxwell.bootstrap, maxwell.heartbeats)
	 * are always passed along.
	 * <p>
	 * The system the blocklist gets rid of the
	 * `ha_health_check` and `rds_heartbeat` tables which are weird
	 * replication-control mechanism events in Alibaba RDS (and maybe amazon?)
	 * <p>
	 * Then we check the configured filters.
	 * <p>
	 * Finally, if we decide to exclude a table we check the filter to
	 * see if it's possible that a column-value filter could reverse this decision
	 *
	 * @param database    The database of the DML
	 * @param table       The table of the DML
	 * @param columnNames Names of the columns this table contains
	 * @return Whether we should write the event to the producer
	 */
	private boolean shouldOutputEvent(String database, String table, Set<String> columnNames) {
		if (Filter.isSystemBlocklisted(database, table)) {
			return false;
		}
		if (filter.isSystemAllowlist(database, table)) {
			return true;
		}
		if (Filter.includes(filter, database, table)) {
			return true;
		}
		if (Objects.isNull(columnNames)) {
			return true;
		}
		return Filter.couldIncludeFromColumnFilters(filter, database, table, columnNames);
	}


	/**
	 * Parse a DDL statement and output the results to the producer
	 */
	public void processQueryEvent(BinlogConnectorEvent event, AbstractProducer producer) throws InvalidSchemaError, SchemaStoreException {
		QueryEventData data = event.queryData();
		String sql = data.getSql();
		Long timestamp = event.getEvent().getHeader().getTimestamp();
		Long lastHeartbeatRead = getLastHeartbeatRead();
		Position position = Position.valueOf(event.getPosition(), lastHeartbeatRead);
		Position nextPosition = Position.valueOf(event.getNextPosition(), lastHeartbeatRead);
		processQueryEvent(data.getDatabase(), sql, position, nextPosition, timestamp, producer);
	}

	/**
	 * Parse a DDL statement and output the results to the producer
	 *
	 * @param dbName       The database "context" under which the SQL is to be processed.  think "use db; alter table foo ..."
	 * @param sql          The DDL SQL to be processed
	 * @param position     The position that the SQL happened at
	 * @param nextPosition The next position that the SQL happened at
	 * @param timestamp    The timestamp of the SQL binlog event
	 */
	private void processQueryEvent(String dbName, String sql, Position position, Position nextPosition, Long timestamp, AbstractProducer producer) throws InvalidSchemaError, SchemaStoreException {
		final Long schemaId = this.schemaStore.getSchemaID();
		schemaStore.processSQL(sql, dbName, position).stream()
				.filter(change -> change.shouldOutput(filter))
				.forEach(change -> {
					DDLMap ddl = new DDLMap(change, timestamp, sql, position, nextPosition, schemaId);
					try {
						if (scripting != null)
							scripting.invoke(ddl);
						producer.push(ddl);
					} catch (Exception e) {
						throw new RuntimeException(e.getMessage(), e);
					}
				});
		tableCache.clear();
	}

	/**
	 * If the input RowMap is one of the heartbeat pulses we sent out,
	 * process it.  If it's one of our heartbeats, we build a `HeartbeatRowMap`,
	 * which will be handled specially in producers (namely, it causes the binlog position to advance).
	 * It is isn't, we leave the row as a RowMap and the rest of the chain will ignore it.
	 *
	 * @return either a RowMap or a HeartbeatRowMap
	 */
	public RowMap processHeartbeats(RowMap row, String clientID) {
		String hbClientID = (String) row.getData("client_id");
		if (!Objects.equals(hbClientID, clientID)) {
			return row; // plain row -- do not process.
		}

		long lastHeartbeatRead = (Long) row.getData("heartbeat");
		LOGGER.debug("replicator picked up heartbeat: {}", lastHeartbeatRead);
		this.lastHeartbeatPosition = row.getPosition().withHeartbeat(lastHeartbeatRead);
		heartbeatNotifier.heartbeat(lastHeartbeatRead);
		return HeartbeatRowMap.valueOf(row.getDatabase(), this.lastHeartbeatPosition, row.getNextPosition().withHeartbeat(lastHeartbeatRead));
	}
}
