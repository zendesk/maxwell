package com.zendesk.maxwell.schema;

import java.sql.*;
import java.util.*;

import java.io.IOException;

import com.github.shyiko.mysql.binlog.GtidSet;

import com.fasterxml.jackson.databind.JavaType;
import com.mysql.cj.xdevapi.Column;
import com.zendesk.maxwell.CaseSensitivity;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.replication.Position;
import com.zendesk.maxwell.schema.columndef.*;

import com.zendesk.maxwell.util.ConnectionPool;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zendesk.maxwell.replication.BinlogPosition;
import com.zendesk.maxwell.schema.ddl.InvalidSchemaError;
import com.zendesk.maxwell.schema.ddl.ResolvedSchemaChange;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;

public class MysqlSavedSchema {
	static int SchemaStoreVersion = 4;

	private Schema schema;
	private Position position;
	private Long schemaID;
	private int schemaVersion;

	private Long baseSchemaID;
	private List<ResolvedSchemaChange> deltas;

	private static final ObjectMapper mapper = new ObjectMapper();
	private static final JavaType listOfResolvedSchemaChangeType = mapper.getTypeFactory().constructCollectionType(List.class, ResolvedSchemaChange.class);

	static final Logger LOGGER = LoggerFactory.getLogger(MysqlSavedSchema.class);

	private final static String columnInsertSQL =
		"INSERT INTO `columns` (schema_id, table_id, name, charset, coltype, is_signed, enum_values, column_length) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

	private final CaseSensitivity sensitivity;
	private final Long serverID;

	private boolean shouldSnapshotNextSchema = false;

	public MysqlSavedSchema(Long serverID, CaseSensitivity sensitivity) throws SQLException {
		this.serverID = serverID;
		this.sensitivity = sensitivity;
	}

	public MysqlSavedSchema(Long serverID, CaseSensitivity sensitivity, Schema schema, Position position) throws SQLException {
		this(serverID, sensitivity);
		this.schema = schema;
		setPosition(position);
	}

	public MysqlSavedSchema(MaxwellContext context, Schema schema, Position position) throws SQLException {
		this(context.getServerID(), context.getCaseSensitivity(), schema, position);
	}

	public MysqlSavedSchema(Long serverID, CaseSensitivity sensitivity, Schema schema, Position position,
							long baseSchemaID, List<ResolvedSchemaChange> deltas) throws SQLException {
		this(serverID, sensitivity, schema, position);

		this.baseSchemaID = baseSchemaID;
		this.deltas = deltas;
	}

	public MysqlSavedSchema createDerivedSchema(Schema newSchema, Position position, List<ResolvedSchemaChange> deltas) throws SQLException {
		if ( this.shouldSnapshotNextSchema )
			return new MysqlSavedSchema(this.serverID, this.sensitivity, newSchema, position);
		else
			return new MysqlSavedSchema(this.serverID, this.sensitivity, newSchema, position, this.schemaID, deltas);
	}

	public Long getSchemaID() {
		return schemaID;
	}

	private static Long executeInsert(PreparedStatement preparedStatement,
			Object... values) throws SQLException {
		for (int i = 0; i < values.length; i++) {
			preparedStatement.setObject(i + 1, values[i]);
		}
		preparedStatement.executeUpdate();

		try ( ResultSet rs = preparedStatement.getGeneratedKeys() ) {
			if (rs.next()) {
				return rs.getLong(1);
			} else
				return null;
		}
	}

	public Long save(Connection connection) throws SQLException {
		if (this.schema == null)
			throw new RuntimeException("Uninitialized schema!");


		this.schemaID = findSchemaForPositionSHA(connection, getPositionSHA());

		if ( this.schemaID != null )
			return schemaID;

		try {
			connection.setAutoCommit(false);
			this.schemaID = saveSchema(connection);
			connection.commit();
		} catch ( SQLIntegrityConstraintViolationException e ) {
			connection.rollback();

			connection.setAutoCommit(true);
			this.schemaID = findSchemaForPositionSHA(connection, getPositionSHA());
		} finally {
			connection.setAutoCommit(true);
		}
		return schemaID;
	}

	/* Look for SHAs already created at a position we're about to save to.
	 * don't conflict with other maxwell replicators running on the same server. */
	private Long findSchemaForPositionSHA(Connection c, String sha) throws SQLException {
		try ( PreparedStatement p = c.prepareStatement("SELECT * from `schemas` where position_sha = ?") ) {
			p.setString(1, sha);
			try ( ResultSet rs = p.executeQuery() ) {

				if ( rs.next() ) {
					Long id = rs.getLong("id");
					LOGGER.debug("findSchemaForPositionSHA: found schema_id: {} for sha: {}", id, sha);
					return id;
				} else {
					return null;
				}
			}
		}
	}

	private Long saveDerivedSchema(Connection conn) throws SQLException {
		try ( PreparedStatement insert = conn.prepareStatement(
				"INSERT into `schemas` SET base_schema_id = ?, deltas = ?, binlog_file = ?, " +
				"binlog_position = ?, server_id = ?, charset = ?, version = ?, " +
				"position_sha = ?, gtid_set = ?, last_heartbeat_read = ?",
				Statement.RETURN_GENERATED_KEYS);) {


			String deltaString;

			try {
				deltaString = mapper.writerFor(listOfResolvedSchemaChangeType).writeValueAsString(deltas);
			} catch ( JsonProcessingException e ) {
				throw new RuntimeException("Couldn't serialize " + deltas + " to JSON.", e);
			}
			BinlogPosition binlogPosition = position.getBinlogPosition();

			return executeInsert(
					insert,
					this.baseSchemaID,
					deltaString,
					binlogPosition.getFile(),
					binlogPosition.getOffset(),
					serverID,
					schema.getCharset(),
					SchemaStoreVersion,
					getPositionSHA(),
					binlogPosition.getGtidSetStr(),
					position.getLastHeartbeatRead()
			);
		}

	}

	public Long saveSchema(Connection conn) throws SQLException {
		if (this.baseSchemaID != null)
			return saveDerivedSchema(conn);

		final Long schemaId;
		try ( PreparedStatement schemaInsert = conn.prepareStatement(
				"INSERT INTO `schemas` SET binlog_file = ?, binlog_position = ?, server_id = ?, charset = ?, version = ?, position_sha = ?, gtid_set = ?, last_heartbeat_read = ?",
				Statement.RETURN_GENERATED_KEYS) ) {

			BinlogPosition binlogPosition = position.getBinlogPosition();
			schemaId = executeInsert(schemaInsert, binlogPosition.getFile(),
					binlogPosition.getOffset(), serverID, schema.getCharset(), SchemaStoreVersion,
					getPositionSHA(), binlogPosition.getGtidSetStr(), position.getLastHeartbeatRead());
		}
		saveFullSchema(conn, schemaId);
		return schemaId;
	}

	public void saveFullSchema(Connection conn, Long schemaId) throws SQLException {

		try ( PreparedStatement databaseInsert = conn.prepareStatement(
				"INSERT INTO `databases` SET schema_id = ?, name = ?, charset=?",
				Statement.RETURN_GENERATED_KEYS);
			  PreparedStatement tableInsert = conn.prepareStatement(
			    "INSERT INTO `tables` SET schema_id = ?, database_id = ?, name = ?, charset=?, pk=?",
			    Statement.RETURN_GENERATED_KEYS) ) {

			ArrayList<Object> columnData = new ArrayList<Object>();

			for (Database d : schema.getDatabases()) {
				Long dbId = executeInsert(databaseInsert, schemaId, d.getName(), d.getCharset());

				for (Table t : d.getTableList()) {
					Long tableId = executeInsert(tableInsert, schemaId, dbId, t.getName(), t.getCharset(), t.getPKString());


					for (ColumnDef c : t.getColumnList()) {
						String enumValuesSQL = null;

						if ( c instanceof EnumeratedColumnDef ) {
							EnumeratedColumnDef enumColumn = (EnumeratedColumnDef) c;
							if (enumColumn.getEnumValues() != null) {
								try {
									enumValuesSQL = mapper.writeValueAsString(enumColumn.getEnumValues());
								} catch (JsonProcessingException e) {
									throw new SQLException(e);
								}
							}
						}

						columnData.add(schemaId);
						columnData.add(tableId);
						columnData.add(c.getName());

						if ( c instanceof StringColumnDef ) {
							columnData.add(((StringColumnDef) c).getCharset());
						} else {
							columnData.add(null);
						}

						columnData.add(c.getType());

						if ( c instanceof IntColumnDef ) {
							columnData.add(((IntColumnDef) c).isSigned() ? 1 : 0);
						} else if ( c instanceof BigIntColumnDef ) {
							columnData.add(((BigIntColumnDef) c).isSigned() ? 1 : 0);
						} else {
							columnData.add(0);
						}

						columnData.add(enumValuesSQL);

						if ( c instanceof ColumnDefWithLength ) {
							Long columnLength = ((ColumnDefWithLength) c).getColumnLength();
							columnData.add(columnLength);
						} else {
							columnData.add(null);
						}
					}

					if ( columnData.size() > 1000 )
						executeColumnInsert(conn, columnData);

				}
			}
			if ( columnData.size() > 0 )
				executeColumnInsert(conn, columnData);
		}
	}

	private void executeColumnInsert(Connection conn, ArrayList<Object> columnData) throws SQLException {
		String insertColumnSQL = this.columnInsertSQL;

		for (int i=1; i < columnData.size() / 8; i++) {
			insertColumnSQL = insertColumnSQL + ", (?, ?, ?, ?, ?, ?, ?, ?)";
		}

		try ( PreparedStatement columnInsert = conn.prepareStatement(insertColumnSQL) ) {
			int i = 1;

			for (Object o : columnData)
				columnInsert.setObject(i++,  o);

			columnInsert.execute();
		}
		columnData.clear();
	}

	public static MysqlSavedSchema restore(MaxwellContext context, Position targetPosition) throws SQLException, InvalidSchemaError {
		return restore(context.getMaxwellConnectionPool(), context.getServerID(), context.getCaseSensitivity(), targetPosition);
	}

	public static MysqlSavedSchema restore(
		ConnectionPool pool,
		Long serverID,
		CaseSensitivity caseSensitivity,
		Position targetPosition
	) throws SQLException, InvalidSchemaError {
		try ( Connection conn = pool.getConnection() ) {
			Long schemaID = findSchema(conn, targetPosition, serverID);
			if (schemaID == null)
				return null;

			MysqlSavedSchema savedSchema = new MysqlSavedSchema(serverID, caseSensitivity);

			savedSchema.restoreFromSchemaID(conn, schemaID);
			savedSchema.handleVersionUpgrades(pool);

			return savedSchema;
		}
	}

	public static MysqlSavedSchema restoreFromSchemaID(
			Long schemaID, Connection conn, CaseSensitivity sensitivity
	) throws SQLException, InvalidSchemaError {
		MysqlSavedSchema savedSchema = new MysqlSavedSchema(schemaID, sensitivity);
		savedSchema.restoreFromSchemaID(conn, schemaID);
		return savedSchema;
	}

	private List<ResolvedSchemaChange> parseDeltas(String json) {
		if ( json == null )
			return null;

		try {
			return mapper.readerFor(listOfResolvedSchemaChangeType).readValue(json.getBytes());
		} catch ( IOException e ) {
			throw new RuntimeException("couldn't parse json delta: " + json.getBytes(), e);
		}
	}

	/* build up a map-of-maps from schema_id -> { col -> val } */
	private HashMap<Long, HashMap<String, Object>> buildSchemaMap(Connection conn) throws SQLException {
		HashMap<Long, HashMap<String, Object>> schemas = new HashMap<>();

		try ( PreparedStatement p = conn.prepareStatement("SELECT * from `schemas`");
			  ResultSet rs = p.executeQuery() ) {
			ResultSetMetaData md = rs.getMetaData();
			while ( rs.next() ) {
				HashMap<String, Object> row = new HashMap<>();
				for ( int i = 1; i <= md.getColumnCount(); i++ )
					row.put(md.getColumnName(i), rs.getObject(i));
				schemas.put(rs.getLong("id"), row);
			}
			return schemas;
		}
	}

	/*
		builds a linked list of schema_ids in which the head of the list
		is the fullly-captured inital schema, and the tail is the final
		delta-schema
	 */

	private LinkedList<Long> buildSchemaChain(HashMap<Long, HashMap<String, Object>> schemas, Long schema_id) {
		LinkedList<Long> schemaChain = new LinkedList<>();

		while ( schema_id != null ) {
			if ( !schemas.containsKey(schema_id) )
				throw new RuntimeException("Couldn't find chained schema: " + schema_id);

			schemaChain.addFirst(schema_id);

			schema_id = (Long) schemas.get(schema_id).get("base_schema_id");
		}
		return schemaChain;
	}

	private void restoreDerivedSchema(Connection conn, Long schema_id) throws SQLException, InvalidSchemaError {
		/* build hashmap of schemaID -> schema properties (as hash) */
		HashMap<Long, HashMap<String, Object>> schemas = buildSchemaMap(conn);

		/* walk backwards to build linked list with base schema at the
		 * head, and the rest of the delta schemas following */
		LinkedList<Long> schemaChain = buildSchemaChain(schemas, schema_id);

		Long firstSchemaId = schemaChain.removeFirst();

		/* do the "full" restore of the schema snapshot */
		MysqlSavedSchema firstSchema = new MysqlSavedSchema(serverID, sensitivity);
		firstSchema.restoreFromSchemaID(conn, firstSchemaId);
		Schema schema = firstSchema.getSchema();

		LOGGER.info("beginning to play deltas...");
		int count = 0;
		long startTime = System.currentTimeMillis();

		/* now walk the chain and play each schema's deltas on top of the snapshot */
		for ( Long id : schemaChain ) {
			List<ResolvedSchemaChange> deltas = parseDeltas((String) schemas.get(id).get("deltas"));
			for ( ResolvedSchemaChange delta : deltas ) {
				delta.apply(schema);
			}
			count++;
		}

		this.schema = schema;
		long elapsed = System.currentTimeMillis() - startTime;
		LOGGER.info("played " + count + " deltas in " + elapsed + "ms");
	}

	protected void restoreFromSchemaID(Connection conn, Long schemaID) throws SQLException, InvalidSchemaError {
		restoreSchemaMetadata(conn, schemaID);

		if (this.baseSchemaID != null) {
			LOGGER.debug("Restoring derived schema");
			restoreDerivedSchema(conn, schemaID);
		} else {
			LOGGER.debug("Restoring full schema");
			restoreFullSchema(conn, schemaID);
		}
	}

	private void restoreSchemaMetadata(Connection conn, Long schemaID) throws SQLException {
		try ( PreparedStatement p = conn.prepareStatement("select * from `schemas` where id = " + schemaID);
			  ResultSet schemaRS = p.executeQuery() ) {

			schemaRS.next();

			setPosition(new Position(
					new BinlogPosition(
							schemaRS.getString("gtid_set"),
							null,
							schemaRS.getInt("binlog_position"),
							schemaRS.getString("binlog_file")
					), schemaRS.getLong("last_heartbeat_read")
			));

			LOGGER.info("Restoring schema id " + schemaRS.getLong("id") + " (last modified at " + this.position + ")");

			this.schemaID = schemaRS.getLong("id");
			this.baseSchemaID = schemaRS.getLong("base_schema_id");

			if ( schemaRS.wasNull() )
				this.baseSchemaID = null;

			this.deltas = parseDeltas(schemaRS.getString("deltas"));
			this.schemaVersion = schemaRS.getInt("version");
			this.schema = new Schema(new ArrayList<Database>(), schemaRS.getString("charset"), this.sensitivity);
		}
	}


	private void restoreFullSchema(Connection conn, Long schemaID) throws SQLException, InvalidSchemaError {
		String sql =
				"SELECT " +
						"d.id AS dbId," +
						"d.name AS dbName," +
						"d.charset AS dbCharset," +
						"t.name AS tableName," +
						"t.charset AS tableCharset," +
						"t.pk AS tablePk," +
						"t.id AS tableId," +
						"c.column_length AS columnLength," +
						"c.enum_values AS columnEnumValues," +
						"c.name AS columnName," +
						"c.charset AS columnCharset," +
						"c.coltype AS columnColtype," +
						"c.is_signed AS columnIsSigned " +
						"FROM `databases` d " +
						"LEFT JOIN tables t ON d.id = t.database_id " +
						"LEFT JOIN columns c ON c.table_id=t.id " +
						"WHERE d.schema_id = ? " +
						"ORDER BY d.id, t.id, c.id";
		try ( PreparedStatement p = conn.prepareStatement(sql) ) {
			p.setLong(1, this.schemaID);
			try ( ResultSet rs = p.executeQuery() ) {

				Database currentDatabase = null;
				Table currentTable = null;
				short columnIndex = 0;
				ArrayList<ColumnDef> columns = new ArrayList<>();

				while (rs.next()) {
					// Database
					String dbName = rs.getString("dbName");
					String dbCharset = rs.getString("dbCharset");

					// Table
					String tName = rs.getString("tableName");
					String tCharset = rs.getString("tableCharset");
					String tPKs = rs.getString("tablePk");

					// Column
					String columnName = rs.getString("columnName");
					int columnLengthInt = rs.getInt("columnLength");
					String columnEnumValues = rs.getString("columnEnumValues");
					String columnCharset = rs.getString("columnCharset");
					String columnType = rs.getString("columnColtype");
					int columnIsSigned = rs.getInt("columnIsSigned");

					if (currentDatabase == null || !currentDatabase.getName().equals(dbName)) {
						if ( currentTable != null ) {
							currentTable.addColumns(columns);
							columns.clear();
						}

						currentDatabase = new Database(dbName, dbCharset);
						this.schema.addDatabase(currentDatabase);
						// make sure two tables named the same in different dbs are picked up.
						currentTable = null;
						LOGGER.debug("Restoring database {}...", dbName);
					}

					if (tName == null) {
						// if tName is null, there are no tables connected to this database
						continue;
					} else if (currentTable == null || !currentTable.getName().equals(tName)) {
						if ( currentTable != null ) {
							currentTable.addColumns(columns);
							columns.clear();
						}

						currentTable = currentDatabase.buildTable(tName, tCharset);
						if (tPKs != null) {
							List<String> pkList = Arrays.asList(StringUtils.split(tPKs, ','));
							currentTable.setPKList(pkList);
						}
						columnIndex = 0;
					}


					if (columnName == null) {
						// If columnName is null, there are no columns connected to this table
						continue;
					}

					Long columnLength;
					if (rs.wasNull()) {
						columnLength = null;
					} else {
						columnLength = (long) columnLengthInt;
					}

					String[] enumValues = null;
					if (columnEnumValues != null) {
						if (this.schemaVersion >= 4) {
							try {
								enumValues = mapper.readValue(columnEnumValues, String[].class);
							} catch (IOException e) {
								throw new SQLException(e);
							}
						} else {
							enumValues = StringUtils.splitByWholeSeparatorPreserveAllTokens(columnEnumValues, ",");
						}
					}

					ColumnDef c = ColumnDef.build(
							columnName,
							columnCharset,
							columnType,
							columnIndex++,
							columnIsSigned == 1,
							enumValues,
							columnLength
					);
					columns.add(c);

				}
				if ( currentTable != null )
					currentTable.addColumns(columns);
				LOGGER.debug("Restored all databases");
			}
		}
	}

	private static Long findSchema(Connection connection, Position targetPosition, Long serverID)
			throws SQLException {
		LOGGER.debug("looking to restore schema at target position {}", targetPosition);
		BinlogPosition targetBinlogPosition = targetPosition.getBinlogPosition();
		if (targetBinlogPosition.getGtidSetStr() != null) {
			String sql = "SELECT id, gtid_set from `schemas` "
					+ "WHERE deleted = 0 "
					+ "ORDER BY id desc";
			try ( PreparedStatement s = connection.prepareStatement(sql) ) {
				try ( ResultSet rs = s.executeQuery() ) {
					while (rs.next()) {
						Long id = rs.getLong("id");
						String gtid = rs.getString("gtid_set");
						LOGGER.debug("Retrieving schema at id: {} gtid: {}", id, gtid);
						if (gtid != null) {
							GtidSet gtidSet = GtidSet.parse(gtid);
							if (gtidSet.isContainedWithin(targetBinlogPosition.getGtidSet())) {
								LOGGER.debug("Found contained schema: {}", id);
								return id;
							}
						}
					}
					return null;
				}
			}
		} else {
			// Only consider binlog positions before the target position on the current server.
			// Within those, sort for the latest binlog file, then the latest binlog position.
			String sql = "SELECT id from `schemas` "
					+ "WHERE deleted = 0 "
					+ "AND last_heartbeat_read <= ? AND ("
					+ "(binlog_file < ?) OR "
					+ "(binlog_file = ? and binlog_position < ? and base_schema_id is not null) OR "
					+ "(binlog_file = ? and binlog_position <= ? and base_schema_id is null) "
					+ ") AND server_id = ? "
					+ "ORDER BY last_heartbeat_read DESC, binlog_file DESC, binlog_position DESC limit 1";
			try ( PreparedStatement s = connection.prepareStatement(sql) ) {

				s.setLong(1, targetPosition.getLastHeartbeatRead());
				s.setString(2, targetBinlogPosition.getFile());
				s.setString(3, targetBinlogPosition.getFile());
				s.setLong(4, targetBinlogPosition.getOffset());
				s.setString(5, targetBinlogPosition.getFile());
				s.setLong(6, targetBinlogPosition.getOffset());
				s.setLong(7, serverID);

				try ( ResultSet rs = s.executeQuery() ) {
					if (rs.next()) {
						return rs.getLong("id");
					} else
						return null;
				}
			}
		}
	}

	public Schema getSchema() {
		return this.schema;
	}

	public void setSchema(Schema s) {
		this.schema = s;
	}

	private void setPosition(Position position) {
		this.position = position;
	}

	public static void delete(Connection connection, long schema_id) throws SQLException {
		connection.createStatement().execute("update `schemas` set deleted = 1 where id = " + schema_id);
	}

	public BinlogPosition getBinlogPosition() {
		if (this.position == null) {
			return null;
		}
		return this.position.getBinlogPosition();
	}

	public Position getPosition() {
		return this.position;
	}

	private void fixUnsignedColumns(Schema recaptured) throws SQLException, InvalidSchemaError {
		int unsignedDiffs = 0;

		for ( Pair<Schema.FullColumnDef, Schema.FullColumnDef> pair : schema.matchColumns(recaptured) ) {
			Table schemaTable = pair.getLeft().getTable();
			ColumnDef schemaCol = pair.getLeft().getColumnDef();
			ColumnDef recapturedCol = pair.getRight().getColumnDef();

			if (schemaCol instanceof IntColumnDef) {
				if (recapturedCol != null && recapturedCol instanceof IntColumnDef) {
					if (((IntColumnDef) schemaCol).isSigned() && !((IntColumnDef) recapturedCol).isSigned()) {
						schemaTable.replaceColumn(schemaCol.getPos(), ((IntColumnDef) schemaCol).withSigned(false));
						unsignedDiffs++;
					}
				} else {
					LOGGER.warn("warning: Couldn't check for unsigned integer bug on column " + schemaCol.getName() +
						".  You may want to recapture your schema");
				}
			} else if (schemaCol instanceof BigIntColumnDef) {
				if (recapturedCol != null && recapturedCol instanceof BigIntColumnDef) {
					if (((BigIntColumnDef) schemaCol).isSigned() && !((BigIntColumnDef) recapturedCol).isSigned()) {
						schemaTable.replaceColumn(schemaCol.getPos(), ((BigIntColumnDef) schemaCol).withSigned(false));
					}
					unsignedDiffs++;
				} else {
					LOGGER.warn("warning: Couldn't check for unsigned integer bug on column " + schemaCol.getName() +
						".  You may want to recapture your schema");
				}
			}
		}

		if ( unsignedDiffs > 0 ) {
			/* A little explanation here: we've detected differences in signed-ness between the restored
			 * and the recaptured schema.  99.9% of the time this will be the result of our capture bug.
			 *
			 * We can't however simply re-save the re-captured schema, as the
			 * capture might be ahead of some DDL updates that we'd otherwise
			 * lose.  So we leave a marker so that the next time we save the
			 * schema, we'll purposely break the delta chain and fix the
			 * unsigned columns in the database.
			 * */
			this.shouldSnapshotNextSchema = true;
		}
	}

	private void fixColumnCases(Schema recaptured) throws InvalidSchemaError {
		int caseDiffs = 0;

		for (Pair<Schema.FullColumnDef, Schema.FullColumnDef> pair : schema.matchColumns(recaptured)) {
			Table schemaTable = pair.getLeft().getTable();
			ColumnDef schemaCol = pair.getLeft().getColumnDef();
			ColumnDef recapturedCol = pair.getRight().getColumnDef();

			if (!schemaCol.getName().equals(recapturedCol.getName())) {
				LOGGER.info("correcting column case of `" + schemaCol.getName() + "` to `" + recapturedCol.getName() + "`.  Will save a full schema snapshot after the new DDL update is processed.");
				caseDiffs++;
				schemaTable.replaceColumn(schemaCol.getPos(), schemaCol.withName(recapturedCol.getName()));
			}
		}
	}

	private void fixColumnLength(Schema recaptured) throws InvalidSchemaError {
		int colLengthDiffs = 0;

		for ( Pair<Schema.FullColumnDef, Schema.FullColumnDef> pair : schema.matchColumns(recaptured) ) {
			Table schemaTable = pair.getLeft().getTable();
			ColumnDef schemaCol = pair.getLeft().getColumnDef();
			ColumnDef recapturedCol = pair.getRight().getColumnDef();

			if (schemaCol instanceof ColumnDefWithLength) {
				if (recapturedCol != null && recapturedCol instanceof ColumnDefWithLength) {
					long aColLength = ((ColumnDefWithLength) schemaCol).getColumnLength();
					long bColLength = ((ColumnDefWithLength) recapturedCol).getColumnLength();

					if ( aColLength != bColLength ) {
						colLengthDiffs++;
						LOGGER.info("correcting column length of `" + schemaCol.getName() + "` to " + bColLength + ".  Will save a full schema snapshot after the new DDL update is processed.");
						schemaTable.replaceColumn(schemaCol.getPos(), ((ColumnDefWithLength) schemaCol).withColumnLength(bColLength));
					}
				} else {
					LOGGER.warn("warning: Couldn't check for column length on column " + schemaCol.getName() +
						".  You may want to recapture your schema");
				}
			}

			if ( colLengthDiffs > 0 )
				this.shouldSnapshotNextSchema = true;
		}
	}

	protected void handleVersionUpgrades(ConnectionPool pool) throws SQLException, InvalidSchemaError {
		if ( this.schemaVersion < 3 ) {
			final Schema recaptured;
			try (Connection conn = pool.getConnection();
				 SchemaCapturer sc = new SchemaCapturer(conn, sensitivity)) {
				recaptured = sc.capture();
			}

			if ( this.schemaVersion < 1 ) {
				if ( this.schema != null && this.schema.findDatabase("mysql") == null ) {
					LOGGER.info("Could not find mysql db, adding it to schema");
					try (Connection conn = pool.getConnection();
						 SchemaCapturer sc = new SchemaCapturer(conn, sensitivity, "mysql")) {
						Database db = sc.capture().findDatabase("mysql");
						this.schema.addDatabase(db);
						this.shouldSnapshotNextSchema = true;
					}
				}

				fixUnsignedColumns(recaptured);
			}

			if ( this.schemaVersion < 2 ) {
				fixColumnCases(recaptured);
			}

			if ( this.schemaVersion < 3 ) {
				fixColumnLength(recaptured);
			}
		}
	}

	private String getPositionSHA() {
		return getSchemaPositionSHA(serverID, position);
	}

	public static String getSchemaPositionSHA(Long serverID, Position position) {
		BinlogPosition binlogPosition = position.getBinlogPosition();
		String shaString = String.format("%d/%s/%d/%d",
			serverID,
			binlogPosition.getFile(),
			binlogPosition.getOffset(),
			position.getLastHeartbeatRead()
		);
		return DigestUtils.shaHex(shaString);
	}

	public Long getBaseSchemaID() {
		return baseSchemaID;
	}

}
