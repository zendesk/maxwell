package com.zendesk.maxwell.replication;

import com.codahale.metrics.Histogram;
import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.BinaryLogFileReader;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.QueryEventData;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.deserialization.ChecksumType;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import com.zendesk.maxwell.Maxwell;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.MaxwellFileReader;
import com.zendesk.maxwell.MaxwellMysqlConfig;
import com.zendesk.maxwell.bootstrap.AbstractBootstrapper;
import com.zendesk.maxwell.monitoring.Metrics;
import com.zendesk.maxwell.producer.AbstractProducer;
import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.row.RowMapBuffer;
import com.zendesk.maxwell.schema.Schema;
import com.zendesk.maxwell.schema.SchemaStore;
import com.zendesk.maxwell.schema.SchemaStoreException;
import com.zendesk.maxwell.schema.Table;

import java.io.FileInputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import static java.util.logging.Level.INFO;

public class BinlogConnectorFileReader extends AbstractReplicator implements
		Replicator {
	private static String className = BinlogConnectorFileReader.class.getName();
	private static Logger logger = Logger.getLogger(className);
	
	private final long MAX_TX_ELEMENTS = 10000;
	protected SchemaStore schemaStore;

	private final boolean stopOnEOF;
	private boolean hitEOF = false;
	
	private BinaryLogFileReader reader = null;
	EventDeserializer ed = new EventDeserializer();
	
	private String binFile = null;
	private Long binPos = null;
	
	ArrayList<String> fileList = null;
	String filePath = null;

	public BinlogConnectorFileReader(SchemaStore schemaStore,
			AbstractProducer producer, AbstractBootstrapper bootstrapper,
			MaxwellMysqlConfig mysqlConfig, Long replicaServerID,
			String maxwellSchemaDatabaseName, Metrics metrics, Position start,
			boolean stopOnEOF, String clientID,
			HeartbeatNotifier heartbeatNotifier, ArrayList<String> fl, String filePath) throws Exception {
		super(clientID, bootstrapper, maxwellSchemaDatabaseName, producer,
				metrics, start, heartbeatNotifier);
		this.schemaStore = schemaStore;
		
		this.filePath = filePath;		
		this.fileList = fl;
		
		BinlogPosition startBinlog = start.getBinlogPosition();
		binFile = startBinlog.getFile();
		binPos = startBinlog.getOffset();
		logger.log(INFO,"Setting initial binlog pos to: "
				+ binFile + ":" + binPos);
		
    	ed.setChecksumType(ChecksumType.CRC32);
    	ed.setCompatibilityMode(
				EventDeserializer.CompatibilityMode.DATE_AND_TIME_AS_LONG_MICRO,
				EventDeserializer.CompatibilityMode.CHAR_AND_BINARY_AS_BYTE_ARRAY);
    	
    	String fn = filePath + fileList.remove(0);
    	logger.log(INFO,"read file " + fn);
    	reader = new BinaryLogFileReader(new FileInputStream(fn), ed);
    	this.stopOnEOF = stopOnEOF;
    	
    	long cnt = rowCounter.getCount();
    	logger.log(INFO,"Initial cnt: " + cnt);
	}

	public BinlogConnectorFileReader(SchemaStore schemaStore,
			AbstractProducer producer, AbstractBootstrapper bootstrapper,
			MaxwellContext ctx, Position start, ArrayList<String> fl) throws Exception {
		this(schemaStore, producer, bootstrapper,
				ctx.getConfig().replicationMysql,
				ctx.getConfig().replicaServerID, ctx.getConfig().databaseName,
				ctx.getMetrics(), start, false, ctx.getConfig().clientID, ctx
						.getHeartbeatNotifier(), fl, ctx.getConfig().filePath);
	}

	private static Pattern createTablePattern = Pattern.compile(
			"^CREATE\\s+TABLE", Pattern.CASE_INSENSITIVE);

	/**
	 * Get a batch of rows for the current transaction.
	 * 
	 * We assume the replicator has just processed a "BEGIN" event, and now
	 * we're inside a transaction. We'll process all rows inside that
	 * transaction and turn them into RowMap objects. We do this because mysql
	 * attaches the transaction-id (xid) to the COMMIT event (at the end of the
	 * transaction), so we process the entire transaction in order to assign
	 * each row the same xid.
	 * 
	 * @return A RowMapBuffer of rows; either in-memory or on disk.
	 */
	private RowMapBuffer getTransactionRows(BinlogConnectorEvent beginEvent)
			throws Exception {
		BinlogConnectorEvent event = null;
		RowMapBuffer buffer = new RowMapBuffer(MAX_TX_ELEMENTS);
			
		while (true) {
			Event ev = null;
			if ( (beginEvent.getType() == EventType.WRITE_ROWS || beginEvent.getType() == EventType.UPDATE_ROWS 
					|| beginEvent.getType() == EventType.DELETE_ROWS || beginEvent.getType() == EventType.EXT_WRITE_ROWS 
					|| beginEvent.getType() == EventType.EXT_UPDATE_ROWS || beginEvent.getType() == EventType.EXT_DELETE_ROWS) && (event == null) ) {
				event = beginEvent;
			} else {
			    ev = reader.readEvent();
			    if (ev == null) {
					//ensureReplicatorThread();
					continue;
				}
				event = new BinlogConnectorEvent(ev,
						binFile, "", "");
			}
			
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
				Table table = tableCache.getTable(event.getTableID());

				if (table != null
						&& shouldOutputEvent(table.getDatabase(),
								table.getName(), filter)) {
					for (RowMap r : event
							.jsonMaps(table, lastHeartbeatPosition))
						buffer.add(r);
				}

				break;
			case TABLE_MAP:
				TableMapEventData data = event.tableMapData();
				tableCache.processEvent(getSchema(), this.filter,
						data.getTableId(), data.getDatabase(), data.getTable());
				break;
			case QUERY:
				QueryEventData qe = event.queryData();
				String sql = qe.getSql();

				if (sql.toUpperCase()
						.startsWith(BinlogConnectorEvent.SAVEPOINT)) {
					logger.log(INFO,"Ignoring SAVEPOINT in transaction: " + qe);
				} else if (createTablePattern.matcher(sql).find()) {
					// CREATE TABLE `foo` SELECT * FROM `bar` will put a CREATE
					// TABLE
					// inside a transaction. Note that this could, in rare
					// cases, lead
					// to us starting on a WRITE_ROWS event -- we sync the
					// schema position somewhere
					// kinda unsafe.
					processQueryEvent(event);
				} else if (sql.toUpperCase().startsWith(
						"INSERT INTO MYSQL.RDS_HEARTBEAT")) {
					// RDS heartbeat events take the following form:
					// INSERT INTO mysql.rds_heartbeat2(id, value) values
					// (1,1483041015005) ON DUPLICATE KEY UPDATE value =
					// 1483041015005
					// We don't need to process them, just ignore
				} else {
					logger.log(INFO, "Unhandled QueryEvent inside transaction: "
							+ qe);
				}
				break;
			}
		}
	}

	private RowMapBuffer rowBuffer;

	/**
	 * The main entry point into the event reading loop.
	 * 
	 * We maintain a buffer of events in a transaction, and each subsequent call
	 * to `getRow` can grab one from the buffer. If that buffer is empty, we'll
	 * go check the open-replicator buffer for rows to process. If that buffer
	 * is empty, we return null.
	 * 
	 * @return either a RowMap or null
	 */
	public RowMap getRow() throws Exception {
		while (true) {
			//logger.log(INFO,"Event" + ev);
			if (rowBuffer != null && !rowBuffer.isEmpty()) {
				RowMap row = rowBuffer.removeFirst();

				if (row != null && isMaxwellRow(row)
						&& row.getTable().equals("heartbeats"))
					return processHeartbeats(row);
				else					
					return row;
			}
			Event ev = reader.readEvent();
			if (null == ev) {
				long cnt = this.rowCounter.getCount();
				if (fileList.size() == 0) {
					logger.log(INFO,"All files are read, stop maxwell. Total row processed:" + cnt);
					this.requestStop();
					return null;
				}
				String nextFile = fileList.remove(0);
				logger.log(INFO,"Switch to nextFile: " + nextFile + " row count:" + cnt);
				/*
				try {
					nextFile = fileList.remove(0);
					logger.log(INFO,"Switch to nextFile: " + nextFile);
				} catch (java.lang.IndexOutOfBoundsException e) {					
					
					logger.log(INFO,"All files are read, stop maxwell. Total row processed:" + cnt);
					this.requestStop();
					return null;
				}*/
				reader = new BinaryLogFileReader(new FileInputStream(
						filePath + nextFile), ed);
				return null;
			}
			
			EventHeaderV4 hV4 = (EventHeaderV4) ev.getHeader();
			Long p = hV4.getPosition();
			if ( p < binPos ) {
				//logger.log(INFO,"Leon skipped:" + p + ev);
				continue;				
			}
			BinlogConnectorEvent ep = new BinlogConnectorEvent(ev,
					binFile, "", "");
			
			switch (ep.getType()) {
			case WRITE_ROWS:
			case EXT_WRITE_ROWS:
			case UPDATE_ROWS:
			case EXT_UPDATE_ROWS:
			case DELETE_ROWS:
			case EXT_DELETE_ROWS:
				logger.log(INFO,"Started replication stream inside a transaction.  This shouldn't normally happen.");
				logger.log(INFO,"Assuming new transaction at unexpected event:"
						+ ev);
				//need update?

				rowBuffer = getTransactionRows(ep);
				break;
			case TABLE_MAP:
				TableMapEventData data = ep.tableMapData();
				tableCache.processEvent(getSchema(), this.filter,
						data.getTableId(), data.getDatabase(), data.getTable());
				break;
			case QUERY:
				QueryEventData qe = ep.queryData();
				String sql = qe.getSql();
				if (BinlogConnectorEvent.BEGIN.equals(sql)) {
					rowBuffer = getTransactionRows(ep);
					rowBuffer.setServerId(ep.getEvent().getHeader()
							.getServerId());
					rowBuffer.setThreadId(qe.getThreadId());
				} else {
					processQueryEvent(ep);
				}
				break;
			case ROTATE:
				tableCache.clear();
				break;
			default:
				break;
			}

		}
		
		//return null;
	}

	private void processQueryEvent(BinlogConnectorEvent event) throws Exception {
		QueryEventData data = event.queryData();
		processQueryEvent(data.getDatabase(), data.getSql(), this.schemaStore,
				lastHeartbeatPosition.withBinlogPosition(event.getPosition()),
				event.getEvent().getHeader().getTimestamp());
	}

	public Schema getSchema() throws SchemaStoreException {
		return this.schemaStore.getSchema();
	}

	@Override
	public void startReplicator() throws Exception {
		// TODO Auto-generated method stub
		
	}
}
