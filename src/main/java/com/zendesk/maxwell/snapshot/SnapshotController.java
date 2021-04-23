package com.zendesk.maxwell.snapshot;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TimeZone;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.jline.utils.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.errors.DuplicateProcessException;
import com.zendesk.maxwell.producer.AbstractProducer;
import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.schema.MysqlSchemaStore;
import com.zendesk.maxwell.schema.Schema;
import com.zendesk.maxwell.schema.SchemaChangeListener;
import com.zendesk.maxwell.schema.SchemaStoreException;
import com.zendesk.maxwell.schema.SchemaStoreSchema;
import com.zendesk.maxwell.schema.Table;
import com.zendesk.maxwell.schema.columndef.ColumnDef;
import com.zendesk.maxwell.schema.columndef.ColumnDefCastException;
import com.zendesk.maxwell.schema.columndef.DateColumnDef;
import com.zendesk.maxwell.schema.columndef.DateTimeColumnDef;
import com.zendesk.maxwell.schema.columndef.TimeColumnDef;
import com.zendesk.maxwell.schema.columndef.YearColumnDef;
import com.zendesk.maxwell.schema.ddl.InvalidSchemaError;
import com.zendesk.maxwell.schema.ddl.ResolvedSchemaChange;

/**
 * @author Ivan
 *
 */
public class SnapshotController implements SchemaChangeListener {

	static private final Logger LOGGER = LoggerFactory.getLogger(SnapshotController.class);

	static private Calendar utcCal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));

	static public final String SnapshotTable = "snapshots";

	public static class SnapshotAbortException extends Exception {

		public SnapshotAbortException(String message) {
			super(message);
		}

		/**
		 * 
		 */
		private static final long serialVersionUID = -2528902065859634757L;
	}

	public static void ensureSnapshotSchema(Connection connection, String snapshotDatabaseName)
			throws SQLException, IOException, InvalidSchemaError {
		if (!storeSnapshotExists(connection, snapshotDatabaseName)) {
			createSnapshotDatabase(connection, snapshotDatabaseName);
		}
	}

	private static boolean storeSnapshotExists(Connection connection, String snapshotDatabaseName) throws SQLException {
		Statement s = connection.createStatement();
		ResultSet rs = s.executeQuery("show databases like '" + snapshotDatabaseName + "'");

		if (!rs.next())
			return false;

		rs = s.executeQuery("show tables from `" + snapshotDatabaseName + "` like '" + SnapshotTable + "'");
		return rs.next();
	}

	private static void executeSQLInputStream(Connection connection, InputStream schemaSQL, String snapshotDatabaseName)
			throws SQLException, IOException {
		BufferedReader r = new BufferedReader(new InputStreamReader(schemaSQL));
		String sql = "", line;

		if (snapshotDatabaseName != null) {
			connection.createStatement().execute("CREATE DATABASE IF NOT EXISTS `" + snapshotDatabaseName + "`");
			if (!connection.getCatalog().equals(snapshotDatabaseName))
				connection.setCatalog(snapshotDatabaseName);
		}

		while ((line = r.readLine()) != null) {
			sql += line + "\n";
		}
		for (String statement : StringUtils.splitByWholeSeparator(sql, "\n\n")) {
			if (statement.length() == 0)
				continue;

			connection.createStatement().execute(statement);
		}
	}

	private static void createSnapshotDatabase(Connection connection, String schemaDatabaseName)
			throws SQLException, IOException {
		LOGGER.info("Creating " + schemaDatabaseName + " database");
		executeSQLInputStream(connection,
				SchemaStoreSchema.class.getResourceAsStream("/sql/maxwell_schema_snapshots.sql"), schemaDatabaseName);
	}

	protected final AbstractProducer producer;
	protected final MaxwellContext context;
	protected final String maxwellSchemaDatabaseName;
	protected final MysqlSchemaStore schemaStore;

	private Map<String, SnapshotTask> snapshots;
	private Map<String, SnapshotTask> snapshotTableMap;

	public SnapshotController(MaxwellContext context, AbstractProducer producer, MysqlSchemaStore schemaStore) {
		this.context = context;
		this.producer = producer;
		this.maxwellSchemaDatabaseName = context.getConfig().databaseName;
		this.schemaStore = schemaStore;

		this.snapshots = new HashMap<String, SnapshotTask>();
		this.snapshotTableMap = new HashMap<String, SnapshotTask>();
	}

	public void initialize() throws Exception {

		// Setup all unfinished snapshot tasks
		var resultSet = getIncompleteSnapshots();
		var clientId = context.getConfig().clientID;

		while (resultSet.next()) {

			var snapshotClientId = resultSet.getString("client_id");
			if (!clientId.equals(snapshotClientId)) {
				continue;
			}

			var id = resultSet.getString("id");
			var db = resultSet.getString("database");
			var table = resultSet.getString("table");
			var whereClause = resultSet.getString("where_clause");
			var chunkStart = resultSet.getInt("chunk_start");
			var rowsSent = resultSet.getInt("rows_sent");
			var complete = false;
			var successful = resultSet.getBoolean("successful");
			Instant completedAt = null;
			String completionReason = null;
			var reqComment = resultSet.getString("request_comment");
			var createdAt = resultSet.getTimestamp("created_at").toInstant();

			var schema = schemaStore.getSchema().findDatabase(db).findTable(table);

			var snapshotTask = new SnapshotTask(id, db, table, whereClause, schema, reqComment, createdAt);

			snapshotTask.setChunkStart(chunkStart);
			snapshotTask.setRowsSent(rowsSent);
			snapshotTask.setComplete(complete);
			snapshotTask.setSuccessful(successful);
			snapshotTask.setCompletedAt(completedAt);
			snapshotTask.setCompletionReason(completionReason);

			snapshots.put(snapshotTask.getId(), snapshotTask);
			snapshotTableMap.put(snapshotTask.getFullTableName(), snapshotTask);

			processNextChunk(snapshotTask);
		}
	}

	protected String buildRowIdentity(RowMap row) throws SchemaStoreException {
		List<Object> rowIdData = new ArrayList<Object>();

		var schema = schemaStore.getSchema().findDatabase(row.getDatabase()).findTable(row.getTable());

		for (var pkName : schema.getPKList()) {
			rowIdData.add(row.getData(pkName));
		}

		if (rowIdData.size() == 0) {
			for (var col : row.getData().entrySet()) {

				var value = row.getOldData().containsKey(col.getKey()) ? row.getOldData(col.getKey())
						: row.getData(col.getKey());
				rowIdData.add(value);
			}
		}

		return "[" + rowIdData.stream().map(r -> r.toString()).collect(Collectors.joining(",")) + "]";
	}

	protected ResultSet getIncompleteSnapshots()
			throws NoSuchElementException, SQLException, DuplicateProcessException, URISyntaxException {

		// Build the query string
		final String sql = String.format("SELECT * FROM `%s`.`%s` WHERE complete=? AND client_id=?",
				maxwellSchemaDatabaseName, SnapshotTable);

		var conn = this.context.getMaxwellConnectionPool().getConnection();
		conn.setCatalog(maxwellSchemaDatabaseName);

		var stmt = conn.prepareStatement(sql);
		stmt.setBoolean(1, false);
		stmt.setString(2, context.getConfig().clientID);

		return stmt.executeQuery();
	}

	public void processRow(RowMap r) throws Exception {
		// Process an update to either the low watermark or high watermark
		if (isWatermarkEvent(r)) {
			processWatermarkEvent(r);
			return;
		}

		// Check if a new snapshot was requested
		if (isNewSnapshotEvent(r)) {
			processNewSnapshotEvent(r);
			return;
		}

		// Check if in-range event
		var snapshot = findInRangeSnapshot(r);
		if (snapshot != null) {
			processInRangeChunkEvent(snapshot, r);
		}
	}

	protected SnapshotTask findInRangeSnapshot(RowMap row) {

		var key = row.getDatabase() + "." + row.getTable();
		var snapshot = snapshots.get(key);
		if (snapshot != null && snapshot.isInRange()) {
			return snapshot;
		}

		return null;
	}

	protected boolean isWatermarkEvent(RowMap row) {

		// Validate the correct table is being updated
		if (!maxwellSchemaDatabaseName.equals(row.getDatabase()) || !SnapshotTable.equals(row.getTable())
				|| !"update".equals(row.getRowType())
				|| !context.getConfig().clientID.equals(row.getData("client_id"))) {
			return false;
		}

		// Validate the watermark has changed
		if (row.getData("watermark") == null || !row.getOldData().containsKey("watermark")) {
			return false;
		}

		return true;
	}

	protected boolean isNewSnapshotEvent(RowMap row) {
		if (!maxwellSchemaDatabaseName.equals(row.getDatabase()) || !SnapshotTable.equals(row.getTable())
				|| !"insert".equals(row.getRowType())
				|| !context.getConfig().clientID.equals(row.getData("client_id"))) {
			return false;
		}

		return true;
	}

	public void processNewSnapshotEvent(RowMap r) throws Exception {

		String snapshotId = (String) r.getData().get("id");
		String database = (String) r.getData().get("database");
		String table = (String) r.getData().get("table");
		String whereClause = (String) r.getData().get("where_clause");
		String requestComment = (String) r.getData().get("request_comment");
		Instant createdAt = java.time.Instant.ofEpochMilli(((Long) r.getData().get("created_at")) / 1000);

		var tableSchema = getTableSchema(database, table);

		var snapshot = new SnapshotTask(snapshotId, database, table, whereClause, tableSchema, requestComment,
				createdAt);

		try {
			// Write back any updated values, e.g. schemaId
			updateSnapshot(snapshot);
		} catch (NoSuchElementException ex) {
			Log.error("Failed to create new snapshot, record missing in DB");
			return;
		}

		snapshots.put(snapshot.getId(), snapshot);
		snapshotTableMap.put(snapshot.getFullTableName(), snapshot);

		producer.push(snapshotStartRowMap(snapshot));

		processNextChunk(snapshot);
	}

	protected void processWatermarkEvent(RowMap row) throws Exception {

		String snapshotId = row.getData("id").toString();
		String watermark = row.getData("watermark").toString();

		var snapshot = snapshots.get(snapshotId);

		if (snapshot == null) {
			LOGGER.warn("Failed to process watermark update event, unknown snapshot " + snapshotId);
			return;
		}

		// There will always be one trailing watermark update when a snapshot completes,
		// use it to remove the snapshot
		if (snapshot.isComplete()) {
			removeSnapshot(snapshot);
			return;
		}

		if (watermark.equals(snapshot.getLowWatermark())) {

			if (snapshot.isInRange()) {
				LOGGER.warn("Received low watermark update event for snapshot marked as in-range" + snapshotId);
			}

			snapshot.setInRange(true);
		}

		if (watermark.equals(snapshot.getHighWatermark())) {

			if (!snapshot.isInRange()) {
				LOGGER.warn("Received high watermark update event for snapshot NOT marked as in-range" + snapshotId);
			}

			for (var chunkRow : snapshot.getChunk().values()) {
				producer.push(chunkRow);
			}

			int chunkSize = snapshot.getChunk().size();

			snapshot.increaseChunkStart(chunkSize);
			snapshot.increaseRowsSent(chunkSize);

			try {
				updateSnapshot(snapshot);
			} catch (NoSuchElementException ex) {
				Log.error("Failed to update new snapshot post watermark event, record missing in DB");
				removeSnapshot(snapshot);
				return;
			}

			snapshot.getChunk().clear();
			snapshot.setInRange(false);

			processNextChunk(snapshot);
		}
	}

	protected void processInRangeChunkEvent(SnapshotTask snapshot, RowMap row) throws SchemaStoreException {

		snapshot.getChunk().remove(buildRowIdentity(row));
	}

	protected void processNextChunk(SnapshotTask snapshot) throws Exception {

		try {
			snapshot.setLowWatermark(generateWatermark());
			updateSnapshotWatermark(snapshot.getId(), snapshot.getLowWatermark());
		} catch (NoSuchElementException ex) {
			Log.error("Failed process next snapshot chunk, record missing in DB");
			removeSnapshot(snapshot);
			return;
		}

		try {

			long currentSchemaID = schemaStore.getSchemaID();
			var table = snapshot.getTableSchema();

			var resultSet = getChunkRows(snapshot);

			snapshot.getChunk().clear();

			ColumnDef[] colDefs = table.getColumnList().toArray(new ColumnDef[0]);

			while (resultSet.next()) {
				RowMap row = snapshotEventRowMap(snapshot);

				setRowValues(row, resultSet, colDefs);
				row.setSchemaId(currentSchemaID);

				snapshot.getChunk().put(buildRowIdentity(row), row);
			}

			snapshot.setChunkSize(snapshot.getChunk().size());

		} catch (Exception ex) {
			Log.error("Failed to read snapshot chunk, " + ex.getMessage());

			snapshot.setComplete(true);
			snapshot.setSuccessful(false);
			snapshot.setCompletionReason("Fetch chunk failure");
			removeSnapshot(snapshot);

			updateSnapshot(snapshot);
		}

		// Check if we're done
		if (snapshot.getChunkSize() == 0) {

			snapshot.complete();
			producer.push(snapshotCompleteRowMap(snapshot));

			try {
				updateSnapshot(snapshot);
			} catch (NoSuchElementException ex) {
				Log.warn("Failed up updated completed snapshot, not in DB");
			}

			return;
		}

		try {
			snapshot.setHighWatermark(generateWatermark());
			updateSnapshotWatermark(snapshot.getId(), snapshot.getHighWatermark());
		} catch (NoSuchElementException ex) {
			Log.error("Failed process next snapshot chunk, record missing in DB");
			removeSnapshot(snapshot);
			return;
		}
	}

	protected ResultSet getChunkRows(SnapshotTask snapshot)
			throws NoSuchElementException, SQLException, DuplicateProcessException, URISyntaxException {

		// Build the query string
		String sql = String.format("SELECT * FROM `%s`.`%s`", snapshot.getDb(), snapshot.getTable());

		var where = snapshot.getWhereClause();
		if (where != null && !where.isBlank() && !where.isEmpty()) {
			where = where.trim();
			sql += " WHERE " + where;
		}

		var orderBy = snapshot.getTableSchema().getPKString();

		if (orderBy.isEmpty()) {
			orderBy = snapshot.getTableSchema().getColumnList().stream().map(c -> c.getName())
					.collect(Collectors.joining(","));
		}

		sql += " ORDER BY " + orderBy + " LIMIT " + snapshot.getChunkSize() + " OFFSET " + snapshot.getChunkStart();

		var conn = getStreamingConnection(snapshot.getDb());
		var stmt = createBatchStatement(conn);

		return stmt.executeQuery(sql);
	}

	protected String generateWatermark() {

		return UUID.randomUUID().toString();
	}

	public void updateSnapshot(SnapshotTask snapshot)
			throws NoSuchElementException, SQLException, DuplicateProcessException {

		final String sql = String.format(
				"UPDATE `%s`.`%s` SET chunk_start=?, rows_sent=?, complete=?, successful=?, completed_at=?, completion_reason=? WHERE id=?",
				maxwellSchemaDatabaseName, SnapshotTable);

		Timestamp completedAt = snapshot.getCompletedAt() != null ? Timestamp.from(snapshot.getCompletedAt()) : null;

		this.context.getMaxwellConnectionPool().withSQLRetry(1, (connection) -> {
			PreparedStatement preparedStatement = connection.prepareStatement(sql);
			preparedStatement.setInt(1, snapshot.getChunkStart());
			preparedStatement.setInt(2, snapshot.getRowsSent());
			preparedStatement.setBoolean(3, snapshot.isComplete());
			preparedStatement.setBoolean(4, snapshot.isSuccessful());
			preparedStatement.setTimestamp(5, completedAt);
			preparedStatement.setString(6, snapshot.getCompletionReason());
			preparedStatement.setString(7, snapshot.getId());

			if (preparedStatement.executeUpdate() == 0) {
				throw new NoSuchElementException();
			}
		});
	}

	protected void updateSnapshotWatermark(String snapshotId, String watermark)
			throws NoSuchElementException, SQLException, DuplicateProcessException {

		final String sql = String.format("UPDATE `%s`.`%s` SET watermark = ? WHERE id = ?", maxwellSchemaDatabaseName,
				SnapshotTable);

		this.context.getMaxwellConnectionPool().withSQLRetry(1, (connection) -> {
			PreparedStatement preparedStatement = connection.prepareStatement(sql);
			preparedStatement.setString(1, watermark);
			preparedStatement.setString(2, snapshotId);

			if (preparedStatement.executeUpdate() == 0) {
				throw new NoSuchElementException();
			}
		});
	}

	private Table getTableSchema(String databaseName, String tableName) throws SchemaStoreException {

		var db = schemaStore.getSchema().findDatabase(databaseName);

		if (db == null) {
			return null;
		}

		return db.findTable(tableName);
	}

	protected Connection getConnection(String databaseName) throws SQLException {

		Connection conn = context.getReplicationConnection();
		conn.setCatalog(databaseName);
		return conn;
	}

	protected Connection getStreamingConnection(String databaseName) throws SQLException, URISyntaxException {

		Connection conn = DriverManager.getConnection(context.getConfig().replicationMysql.getConnectionURI(false),
				context.getConfig().replicationMysql.user, context.getConfig().replicationMysql.password);
		conn.setCatalog(databaseName);
		return conn;
	}

	protected Statement createBatchStatement(Connection connection) throws SQLException {

		Statement statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		statement.setFetchSize(Integer.MIN_VALUE);
		return statement;
	}

	private RowMap snapshotEventRowMap(SnapshotTask snapshot) {

		var schema = snapshot.getTableSchema();

		RowMap row = new RowMap("snapshot-insert", snapshot.getDb(), snapshot.getTable(), System.currentTimeMillis(),
				schema.getPKList(), snapshot.getId());

		return row;
	}

	private RowMap snapshotStartRowMap(SnapshotTask snapshot) {

		var schema = snapshot.getTableSchema();

		RowMap row = new RowMap("snapshot-start", snapshot.getDb(), snapshot.getTable(), System.currentTimeMillis(),
				schema.getPKList(), snapshot.getId());

		return row;
	}

	private RowMap snapshotCompleteRowMap(SnapshotTask snapshot) {

		var schema = snapshot.getTableSchema();

		RowMap row = new RowMap("snapshot-complete", snapshot.getDb(), snapshot.getTable(), System.currentTimeMillis(),
				schema.getPKList(), snapshot.getId());

		return row;
	}

	private RowMap snapshotAbortedRowMap(SnapshotTask snapshot, String comment) {
		var schema = snapshot.getTableSchema();

		RowMap row = new RowMap("snapshot-aborted", snapshot.getDb(), snapshot.getTable(), System.currentTimeMillis(),
				schema.getPKList(), snapshot.getId());
		row.setComment(comment);

		return row;
	}

	private void setRowValues(RowMap row, ResultSet resultSet, ColumnDef[] colDefs)
			throws SQLException, ColumnDefCastException {

		for (int columnIndex = 1; columnIndex <= colDefs.length; columnIndex++) {
			ColumnDef columnDefinition = colDefs[columnIndex - 1];
			Object columnValue = null;

			if (columnDefinition instanceof TimeColumnDef || columnDefinition instanceof DateColumnDef
					|| columnDefinition instanceof DateTimeColumnDef) {
				var timestamp = resultSet.getTimestamp(columnIndex, utcCal);
				columnValue = timestamp == null ? null : timestamp.getTime() * 1000; // convert to microseconds
			}

			else if (columnDefinition instanceof YearColumnDef)
				columnValue = (int) resultSet.getShort(columnIndex);
			else
				columnValue = resultSet.getObject(columnIndex);

			row.putData(columnDefinition.getName(), columnValue);
		}
	}

	protected void removeSnapshot(SnapshotTask snapshot) {
		snapshots.remove(snapshot.getId());
		snapshotTableMap.remove(snapshot.getFullTableName());
	}

	@Override
	public void onSchemaChange(List<ResolvedSchemaChange> changes, long newSchemaId, Schema newSchema) {
		// Simply cancel all snapshots for tables with a schema change
		for (var change : changes) {
			String tablePath = String.format("%s.%s", change.databaseName(), change.tableName());

			var snapshot = snapshotTableMap.get(tablePath);
			if (snapshot != null) {
				Log.info("Aborting snapshot due to schema change");

				try {
					snapshot.complete(false, "schema change");
					updateSnapshot(snapshot);
				} catch (Exception ex) {
					Log.error("Failed to update snapshot, record missing in DB");
				}

				try {
					producer.push(snapshotAbortedRowMap(snapshot, "schema-change"));
				} catch (Exception ex) {
					Log.error("Failed to publish snapshot-aborted event, record missing in DB");
				}

				removeSnapshot(snapshot);
			}
		}
	}
}
