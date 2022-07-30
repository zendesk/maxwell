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
import com.zendesk.maxwell.schema.SchemaStoreException;
import com.zendesk.maxwell.schema.Table;
import com.zendesk.maxwell.schema.columndef.ColumnDefCastException;
import com.zendesk.maxwell.schema.ddl.DDLMap;
import com.zendesk.maxwell.schema.ddl.InvalidSchemaError;
import com.zendesk.maxwell.util.Logging;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.SQLException;
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

	public static final String HEARTBEATS = "heartbeats";
	public static final String DEFAULT_LOG_LEVEL = "info";

	private static final Pattern CREATE_TABLE_PATTERN = Pattern.compile("^CREATE\\s+TABLE", Pattern.CASE_INSENSITIVE);

	private static final long MAX_TX_ELEMENTS = 10000;
	private final RowMapBuffer rowBuffer = new RowMapBuffer(MAX_TX_ELEMENTS);

	private final ReplayConfig config;

	private final AbstractProducer producer;

	private final TableCache tableCache;

	private final SchemaStore schemaStore;
	private final Schema schema;

	private Position lastHeartbeatPosition;

	private long rowCount;


	public static void main(String[] args) {
		try {
			new MaxwellReplayFile(args).start();
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		}
	}

	private MaxwellReplayFile(String[] args) throws SQLException, URISyntaxException, IOException, SchemaStoreException {
		this.config = new ReplayConfig(args);
		MaxwellContext context = new MaxwellContext(config);

		Logging.setLevel(config.log_level == null ? DEFAULT_LOG_LEVEL : config.log_level);

		this.producer = context.getProducer();
		this.tableCache = new TableCache(config.databaseName);
		this.schemaStore = new ReplayBinlogStore(context.getSchemaConnectionPool(), CaseSensitivity.CONVERT_ON_COMPARE, config);
		this.schema = schemaStore.getSchema();
	}

	public void start() {
		try {
			config.binlogFiles.forEach(this::replayBinlog);
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		} finally {
			daemon();
		}
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

	/**
	 * Replay the binlog, if an error is encountered, the replay will be terminated,
	 * and you need to confirm whether to skip the position to continue execution
	 *
	 * @param binlogFile binlog file
	 */
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

		String position = null;
		try (BinaryLogFileReader reader = new BinaryLogFileReader(binlogFile, eventDeserializer)) {
			RowMap row = getRow(reader, binlogFile.getName());
			while (row != null) {
				if (shouldOutputRowMap(row)) {
					producer.push(row);
					rowCount++;
				}
				position = row.getPosition().getBinlogPosition().fullPosition();

				// continue to get next
				row = getRow(reader, binlogFile.getName());
			}
		} catch (Exception e) {
			throw new RuntimeException("Replay failed, Check from: " + position + ", error: " + e.getMessage(), e);
		} finally {
			LOGGER.info("End replay binlog file: {}", binlogFile.getAbsoluteFile());
		}
	}

	private RowMap getRow(BinaryLogFileReader reader, String binlogName) throws Exception {
		BinlogConnectorEvent event;
		while (rowBuffer.isEmpty()) {
			event = wrapEvent(reader.readEvent(), binlogName);
			if (event == null) {
				return null;
			}

			switch (event.getType()) {
				case WRITE_ROWS:
				case EXT_WRITE_ROWS:
				case UPDATE_ROWS:
				case EXT_UPDATE_ROWS:
				case DELETE_ROWS:
				case EXT_DELETE_ROWS:
					LOGGER.warn("Started replication stream inside a transaction.  This shouldn't normally happen.");
					LOGGER.warn("Assuming new transaction at unexpected event:" + event);

					writeRows(event, reader, binlogName);
					break;
				case TABLE_MAP:
					cacheTable(event.tableMapData());
					break;
				case QUERY:
					QueryEventData qe = event.queryData();
					String sql = qe.getSql();
					if (BinlogConnectorEvent.BEGIN.equals(sql)) {
						writeRows(event, reader, binlogName);

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

		RowMap row = rowBuffer.removeFirst();
		if (row != null && isMaxwellRow(row) && HEARTBEATS.equals(row.getTable())) {
			return processHeartbeats(row);
		}
		return row;
	}

	private BinlogConnectorEvent wrapEvent(Event event, String filename) {
		if (event == null) {
			return null;
		}
		String gtid = null;
		if (event.getHeader().getEventType() == EventType.GTID) {
			gtid = ((GtidEventData) event.getData()).getGtid();
		}
		return new BinlogConnectorEvent(event, filename, null, gtid, config.outputConfig);
	}

	/**
	 * Write data to rowBuffer
	 *
	 * @param beginEvent binlog event
	 * @param reader     binlog reader
	 * @param binlogName binlog name
	 * @throws Exception
	 */
	private void writeRows(BinlogConnectorEvent beginEvent, BinaryLogFileReader reader, String binlogName) throws IOException, ColumnDefCastException, InvalidSchemaError, SchemaStoreException {
		BinlogConnectorEvent event;
		String currentQuery = null;

		if (!Objects.equals(beginEvent.getType(), EventType.QUERY)) {
			// data stack header
			writeRow(beginEvent, null);
		}

		while (true) {
			event = wrapEvent(reader.readEvent(), binlogName);
			if (event == null) {
				LOGGER.warn("Transaction commit not read but event terminated, binlog: {}", binlogName);
				return;
			}

			EventType eventType = event.getEvent().getHeader().getEventType();
			if (event.isCommitEvent()) {
				if (!rowBuffer.isEmpty()) {
					rowBuffer.getLast().setTXCommit();
				}
				if (eventType == EventType.XID) {
					rowBuffer.setXid(event.xidData().getXid());
				}
				return;
			}

			switch (eventType) {
				case WRITE_ROWS:
				case UPDATE_ROWS:
				case DELETE_ROWS:
				case EXT_WRITE_ROWS:
				case EXT_UPDATE_ROWS:
				case EXT_DELETE_ROWS:
					writeRow(event, currentQuery);
//					currentQuery = null;
					break;
				case TABLE_MAP:
					cacheTable(event.tableMapData());
					break;
				case ROWS_QUERY:
					RowsQueryEventData queryEventData = event.getEvent().getData();
					currentQuery = queryEventData.getQuery();
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

	private void cacheTable(TableMapEventData data) {
		if (Filter.isSystemBlacklisted(data.getDatabase(), data.getTable()) || !Filter.includes(config.filter, data.getDatabase(), data.getTable())) {
			// No need for caching
			return;
		}
		tableCache.processEvent(schema, config.filter, data.getTableId(), data.getDatabase(), data.getTable());
	}

	/**
	 * Writes an event's data to the RowBuffer
	 *
	 * @param event binlog event
	 * @param query sql
	 * @throws ColumnDefCastException event conversion RowMap exception
	 */
	private void writeRow(BinlogConnectorEvent event, String query) throws ColumnDefCastException {
		Table table = tableCache.getTable(event.getTableID());
		if (table == null || !shouldOutputEvent(table.getDatabase(), table.getName(), config.filter, table.getColumnNames())) {
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
			throw e;
		}
	}

	private void processQueryEvent(BinlogConnectorEvent event) throws InvalidSchemaError, SchemaStoreException {
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

	private void processQueryEvent(String dbName, String sql, SchemaStore schemaStore, Position position, Position nextPosition, Long timestamp) throws InvalidSchemaError, SchemaStoreException {
		Long schemaId = this.schemaStore.getSchemaID();
		schemaStore.processSQL(sql, dbName, position).stream().filter(change -> change.shouldOutput(config.filter)).forEach(change -> {
			DDLMap ddl = new DDLMap(change, timestamp, sql, position, nextPosition, schemaId);
			try {
				producer.push(ddl);
				rowCount++;
			} catch (Exception e) {
				throw new RuntimeException(e.getMessage(), e);
			}
		});
		tableCache.clear();
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

	private boolean shouldOutputRowMap(RowMap row) {
		String database = row.getDatabase();
		String table = row.getTable();
		return config.filter.isSystemWhitelisted(database, table) || config.filter.includes(database, table, row.getData());
	}
}
