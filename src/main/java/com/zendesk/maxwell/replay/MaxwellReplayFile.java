package com.zendesk.maxwell.replay;

import com.github.shyiko.mysql.binlog.BinaryLogFileReader;
import com.github.shyiko.mysql.binlog.event.*;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import com.zendesk.maxwell.CaseSensitivity;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.filtering.Filter;
import com.zendesk.maxwell.producer.AbstractProducer;
import com.zendesk.maxwell.replication.BinlogConnectorEvent;
import com.zendesk.maxwell.replication.Position;
import com.zendesk.maxwell.replication.TableCache;
import com.zendesk.maxwell.row.HeartbeatRowMap;
import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.row.RowMapBuffer;
import com.zendesk.maxwell.schema.Schema;
import com.zendesk.maxwell.schema.SchemaStore;
import com.zendesk.maxwell.schema.Table;
import com.zendesk.maxwell.schema.columndef.ColumnDefCastException;
import com.zendesk.maxwell.schema.ddl.DDLMap;
import com.zendesk.maxwell.schema.ddl.ResolvedSchemaChange;
import com.zendesk.maxwell.util.Logging;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.locks.LockSupport;
import java.util.regex.Pattern;

/**
 * @author udyr@shlaji.com
 */
public class MaxwellReplayFile {
	private static final Logger LOGGER = LoggerFactory.getLogger(MaxwellReplayFile.class);

	private static final long MAX_TX_ELEMENTS = 10000;
	private static final Pattern CREATE_TABLE_PATTERN = Pattern.compile("^CREATE\\s+TABLE", Pattern.CASE_INSENSITIVE);
	private TableCache tableCache;
	private RowMapBuffer rowBuffer;
	private Position lastHeartbeatPosition;
	private SchemaStore schemaStore;
	private ReplayConfig config;
	private Schema schema;
	private AbstractProducer producer;
	private MaxwellContext context;
	private long rowCount;

	public static void main(String[] args) {
		new MaxwellReplayFile().start(args);
	}

	public void start(String[] args) {
		try {
			config = new ReplayConfig(args);
			context = new MaxwellContext(config);

			Logging.setLevel(config.log_level == null ? "info" : config.log_level);

			producer = context.getProducer();
			tableCache = new TableCache(config.databaseName);

			this.schemaStore = new ReplayBinlogStore(context.getSchemaConnectionPool(), CaseSensitivity.CONVERT_ON_COMPARE, config);
			this.schema = schemaStore.getSchema();

			List<File> files = config.binlogFiles;
			for (File file : files) {
				replayBinlog(file);
			}
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		}

		daemon();
	}

	/**
	 * Wait producer for replay
	 */
	private void daemon() {
		while (!producer.flushAndClose()) {
			LOGGER.debug("waiting produce...");
			LockSupport.parkNanos(1000_000_000L);
		}
		LOGGER.info("complete replay: {}", rowCount);
	}

	private void replayBinlog(File binlogFile) {
		if (!binlogFile.exists()) {
			LOGGER.warn("File does not exist, {}", binlogFile.getAbsoluteFile());
			return;
		}
		LOGGER.info("Start replay binlog file: {}", binlogFile.getAbsoluteFile());
		EventDeserializer eventDeserializer = new EventDeserializer();
		eventDeserializer.setCompatibilityMode(
				EventDeserializer.CompatibilityMode.DATE_AND_TIME_AS_LONG_MICRO,
				EventDeserializer.CompatibilityMode.CHAR_AND_BINARY_AS_BYTE_ARRAY,
				EventDeserializer.CompatibilityMode.INVALID_DATE_AND_TIME_AS_MIN_VALUE
		);

		try (BinaryLogFileReader reader = new BinaryLogFileReader(binlogFile, eventDeserializer)) {
			RowMap row;
			while (true) {
				row = getRow(reader, binlogFile.getName());
				if (row == null) {
					break;
				}
				if (shouldOutputRowMap(row.getDatabase(), row.getTable(), row, config.filter)) {
					producer.push(row);
					rowCount++;
				}
			}
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		} finally {
			LOGGER.info("End replay binlog file: {}", binlogFile.getAbsoluteFile());
		}
	}

	private RowMap getRow(BinaryLogFileReader reader, String binlogName) throws Exception {
		BinlogConnectorEvent event;
		while (true) {
			if (Objects.nonNull(rowBuffer) && !rowBuffer.isEmpty()) {
				RowMap row = rowBuffer.removeFirst();
				if (row != null && isMaxwellRow(row) && "heartbeats".equals(row.getTable())) {
					return processHeartbeats(row);
				}
				return row;
			}
			Event binlogEvent = reader.readEvent();
			if (binlogEvent == null) {
				return null;
			}
			String gtid = null;
			String gtidSet = null;
			if (binlogEvent.getHeader().getEventType() == EventType.GTID) {
				gtid = ((GtidEventData) binlogEvent.getData()).getGtid();
			}
			event = new BinlogConnectorEvent(binlogEvent, binlogName, gtidSet, gtid, config.outputConfig);
			switch (event.getType()) {
				case WRITE_ROWS:
				case EXT_WRITE_ROWS:
				case UPDATE_ROWS:
				case EXT_UPDATE_ROWS:
				case DELETE_ROWS:
				case EXT_DELETE_ROWS:
					LOGGER.warn("Started replication stream inside a transaction.  This shouldn't normally happen.");
					LOGGER.warn("Assuming new transaction at unexpected event:" + event);

					rowBuffer = getTransactionRows(event, reader, binlogName);
					break;
				case TABLE_MAP:
					TableMapEventData data = event.tableMapData();
					if (Filter.isSystemBlacklisted(data.getDatabase(), data.getTable()) || !Filter.includes(config.filter, data.getDatabase(), data.getTable())) {
						// No need for caching
						break;
					}
					tableCache.processEvent(schema, config.filter, data.getTableId(), data.getDatabase(), data.getTable());
					break;
				case QUERY:
					QueryEventData qe = event.queryData();
					String sql = qe.getSql();
					if (BinlogConnectorEvent.BEGIN.equals(sql)) {
						rowBuffer = getTransactionRows(event, reader, binlogName);

						rowBuffer.setServerId(event.getEvent().getHeader().getServerId());
						rowBuffer.setThreadId(qe.getThreadId());
						rowBuffer.setSchemaId(schemaStore.getSchemaID());
					} else {
						processQueryEvent(event);
					}
					break;
				case ROTATE:
					tableCache.clear();
					break;
				default:
					break;
			}
		}
	}


	private RowMapBuffer getTransactionRows(BinlogConnectorEvent beginEvent, BinaryLogFileReader reader, String binlogName) throws Exception {
		BinlogConnectorEvent event;
		final RowMapBuffer buffer = new RowMapBuffer(MAX_TX_ELEMENTS);
		String currentQuery = null;

		if (!Objects.equals(beginEvent.getType(), EventType.QUERY)) {
			// data stack header
			extractData(beginEvent, buffer, null);
		}

		while (true) {
			Event binlogEvent = reader.readEvent();
			if (binlogEvent == null) {
				LOGGER.warn("event end, binlog: {}", binlogName);
				return buffer;
			}

			String gtid = null;
			String gtidSet = null;
			if (binlogEvent.getHeader().getEventType() == EventType.GTID) {
				gtid = ((GtidEventData) binlogEvent.getData()).getGtid();
			}
			event = new BinlogConnectorEvent(binlogEvent, binlogName, gtidSet, gtid, config.outputConfig);

			EventType eventType = event.getEvent().getHeader().getEventType();
			if (event.isCommitEvent()) {
				if (!buffer.isEmpty()) {
					buffer.getLast().setTXCommit();
				}
				if (eventType == EventType.XID) {
					buffer.setXid(event.xidData().getXid());
				}
				return buffer;
			}

			switch (eventType) {
				case WRITE_ROWS:
				case UPDATE_ROWS:
				case DELETE_ROWS:
				case EXT_WRITE_ROWS:
				case EXT_UPDATE_ROWS:
				case EXT_DELETE_ROWS:
					extractData(event, buffer, currentQuery);
//					currentQuery = null;
					break;
				case TABLE_MAP:
					TableMapEventData data = event.tableMapData();
					if (Filter.isSystemBlacklisted(data.getDatabase(), data.getTable()) || !Filter.includes(config.filter, data.getDatabase(), data.getTable())) {
						// No need for caching
						break;
					}
					tableCache.processEvent(schema, config.filter, data.getTableId(), data.getDatabase(), data.getTable());
					break;
				case ROWS_QUERY:
					RowsQueryEventData rqed = event.getEvent().getData();
					currentQuery = rqed.getQuery();
					break;
				case QUERY:
					QueryEventData qe = event.queryData();
					String sql = qe.getSql();
					String upperCaseSql = sql.toUpperCase();

					if (CREATE_TABLE_PATTERN.matcher(sql).find()) {
						processQueryEvent(event);
					}

					if (upperCaseSql.startsWith(BinlogConnectorEvent.SAVEPOINT)) {
						LOGGER.debug("Ignoring SAVEPOINT in transaction: {}", qe);
					} else if (upperCaseSql.startsWith("INSERT INTO MYSQL.RDS_") || upperCaseSql.startsWith("DELETE FROM MYSQL.RDS_")) {
					} else if (upperCaseSql.startsWith("DROP TEMPORARY TABLE")) {
					} else if (upperCaseSql.startsWith("# DUMMY EVENT")) {
					} else {
						LOGGER.warn("Unhandled QueryEvent @ {} inside transaction: {}", event.getPosition().fullPosition(), qe);
					}
					break;
				default:
					break;
			}
		}
	}

	private void extractData(BinlogConnectorEvent event, RowMapBuffer buffer, String query) throws ColumnDefCastException {
		Table table = tableCache.getTable(event.getTableID());
		if (table == null || !shouldOutputEvent(table.getDatabase(), table.getName(), config.filter, table.getColumnNames())) {
			return;
		}

		try {
			List<RowMap> rows = event.jsonMaps(table, getLastHeartbeatRead(), query);
			rows.stream().filter(r -> shouldOutputRowMap(table.getDatabase(), table.getName(), r, config.filter)).forEach(r -> {
				try {
					buffer.add(r);
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			});
		} catch (ColumnDefCastException e) {
			logColumnDefCastException(table, e);
			throw e;
		}
	}

	private void processQueryEvent(BinlogConnectorEvent event) throws Exception {
		QueryEventData data = event.queryData();
		processQueryEvent(
				data.getDatabase(),
				data.getSql(),
				this.schemaStore,
				Position.valueOf(event.getPosition(), getLastHeartbeatRead()),
				Position.valueOf(event.getNextPosition(), getLastHeartbeatRead()),
				event.getEvent().getHeader().getTimestamp()
		);
	}

	private void processQueryEvent(String dbName, String sql, SchemaStore schemaStore, Position position, Position nextPosition, Long timestamp) throws Exception {
		List<ResolvedSchemaChange> changes = schemaStore.processSQL(sql, dbName, position);
		Long schemaId = this.schemaStore.getSchemaID();
		for (ResolvedSchemaChange change : changes) {
			if (change.shouldOutput(config.filter)) {
				DDLMap ddl = new DDLMap(change, timestamp, sql, position, nextPosition, schemaId);
				producer.push(ddl);
				rowCount++;
			}
		}
		tableCache.clear();
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

	private boolean shouldOutputEvent(String database, String table, Filter filter, Set<String> columnNames) {
		if (Filter.isSystemBlacklisted(database, table)) {
			return false;
		}
		if (filter.isSystemWhitelisted(database, table)) {
			return true;
		}
		if (Filter.includes(filter, database, table)) {
			return true;
		}
		return Filter.couldIncludeFromColumnFilters(filter, database, table, columnNames);
	}

	private RowMap processHeartbeats(RowMap row) {
		long lastHeartbeatRead = (Long) row.getData("heartbeat");
		LOGGER.debug("replay picked up heartbeat: {}", lastHeartbeatRead);
		this.lastHeartbeatPosition = row.getPosition().withHeartbeat(lastHeartbeatRead);
		return HeartbeatRowMap.valueOf(row.getDatabase(), this.lastHeartbeatPosition, row.getNextPosition().withHeartbeat(lastHeartbeatRead));
	}

	private boolean isMaxwellRow(RowMap row) {
		return row.getDatabase().equals(config.databaseName);
	}

	private boolean shouldOutputRowMap(String database, String table, RowMap rowMap, Filter filter) {
		return filter.isSystemWhitelisted(database, table) || filter.includes(database, table, rowMap.getData());
	}
}
