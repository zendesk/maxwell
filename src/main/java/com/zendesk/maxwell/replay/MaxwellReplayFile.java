package com.zendesk.maxwell.replay;

import com.github.shyiko.mysql.binlog.BinaryLogFileReader;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.QueryEventData;
import com.github.shyiko.mysql.binlog.event.RowsQueryEventData;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import com.zendesk.maxwell.CaseSensitivity;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.producer.AbstractProducer;
import com.zendesk.maxwell.replication.BinlogConnectorEvent;
import com.zendesk.maxwell.replication.BinlogConnectorEventProcessor;
import com.zendesk.maxwell.replication.HeartbeatNotifier;
import com.zendesk.maxwell.replication.TableCache;
import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.row.RowMapBuffer;
import com.zendesk.maxwell.schema.SchemaStoreException;
import com.zendesk.maxwell.schema.columndef.ColumnDefCastException;
import com.zendesk.maxwell.schema.ddl.InvalidSchemaError;
import com.zendesk.maxwell.util.Logging;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.Objects;
import java.util.concurrent.locks.LockSupport;

import static com.zendesk.maxwell.replication.BinlogConnectorEventProcessor.CREATE_TABLE_PATTERN;

/**
 * @author udyr@shlaji.com
 */
public class MaxwellReplayFile {
	private static final Logger LOGGER = LoggerFactory.getLogger(MaxwellReplayFile.class);

	public static final String HEARTBEATS = "heartbeats";
	public static final String DEFAULT_LOG_LEVEL = "info";


	private static final long MAX_TX_ELEMENTS = 10000;
	private final RowMapBuffer rowBuffer = new RowMapBuffer(MAX_TX_ELEMENTS);

	private final ReplayConfig config;

	private final BinlogConnectorEventProcessor processor;

	private final AbstractProducer producer;

	private long rowCount;


	public static void main(String[] args) {
		try {
			new MaxwellReplayFile(args).start();
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		}
	}

	private MaxwellReplayFile(String[] args) throws SQLException, URISyntaxException, IOException {
		this.config = new ReplayConfig(args);
		MaxwellContext context = new MaxwellContext(config);

		Logging.setLevel(config.log_level == null ? DEFAULT_LOG_LEVEL : config.log_level);

		this.producer = context.getProducer();
		ReplayBinlogStore schemaStore = new ReplayBinlogStore(context.getSchemaConnectionPool(), CaseSensitivity.CONVERT_ON_COMPARE, config);
		this.processor = new BinlogConnectorEventProcessor(new TableCache(), schemaStore, null, config.outputConfig, config.filter, config.scripting, new HeartbeatNotifier());
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
				producer.push(row);
				rowCount++;
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
			event = processor.wrapEvent(reader.readEvent(), binlogName);
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
					processor.cacheTable(event.tableMapData());
					break;
				case QUERY:
					QueryEventData qe = event.queryData();
					String sql = qe.getSql();
					if (BinlogConnectorEvent.BEGIN.equals(sql)) {
						writeRows(event, reader, binlogName);

						rowBuffer.setServerId(event.getEvent().getHeader().getServerId());
						rowBuffer.setThreadId(qe.getThreadId());
						rowBuffer.setSchemaId(processor.getSchemaId());
					} else {
						processor.processQueryEvent(event, producer);
						rowCount++;
					}
					break;
				case ROTATE:
					processor.clearTableCache();
					break;
				default:
					break;
			}
		}

		RowMap row = rowBuffer.removeFirst();
		if (row != null && row.getDatabase().equals(config.databaseName) && HEARTBEATS.equals(row.getTable())) {
			return processor.processHeartbeats(row, null);
		}
		return row;
	}


	/**
	 * Write data to rowBuffer
	 *
	 * @param beginEvent binlog event
	 * @param reader     binlog reader
	 * @param binlogName binlog name
	 */
	private void writeRows(BinlogConnectorEvent beginEvent, BinaryLogFileReader reader, String binlogName) throws IOException, ColumnDefCastException, InvalidSchemaError, SchemaStoreException {
		BinlogConnectorEvent event;
		String currentQuery = null;

		if (!Objects.equals(beginEvent.getType(), EventType.QUERY)) {
			// data stack header
			processor.writeRow(beginEvent, rowBuffer, null);
		}

		while (true) {
			event = processor.wrapEvent(reader.readEvent(), binlogName);
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
					processor.writeRow(event, rowBuffer, currentQuery);
//					currentQuery = null;
					break;
				case TABLE_MAP:
					processor.cacheTable(event.tableMapData());
					break;
				case ROWS_QUERY:
					RowsQueryEventData queryEventData = event.getEvent().getData();
					currentQuery = queryEventData.getQuery();
					break;
				case QUERY:
					QueryEventData qe = event.queryData();
					String sql = qe.getSql();
					if (CREATE_TABLE_PATTERN.matcher(sql).find()) {
						// CREATE TABLE `foo` SELECT * FROM `bar` will put a CREATE TABLE
						// inside a transaction.  Note that this could, in rare cases, lead
						// to us starting on a WRITE_ROWS event -- we sync the schema position somewhere
						// kinda unsafe.
						processor.processQueryEvent(event, producer);
						rowCount++;
					}
					break;
				default:
					break;
			}
		}
	}
}
