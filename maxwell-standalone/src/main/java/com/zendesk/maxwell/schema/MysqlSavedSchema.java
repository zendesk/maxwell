package com.zendesk.maxwell.schema;

import java.sql.*;
import java.util.*;

import java.io.IOException;

import com.github.shyiko.mysql.binlog.GtidSet;
import com.mysql.jdbc.exceptions.jdbc4.MySQLIntegrityConstraintViolationException;

import com.fasterxml.jackson.databind.JavaType;
import com.zendesk.maxwell.CaseSensitivity;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.replication.Position;
import com.zendesk.maxwell.schema.columndef.*;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.text.StrTokenizer;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zendesk.maxwell.replication.BinlogPosition;
import com.zendesk.maxwell.schema.ddl.InvalidSchemaError;
import com.zendesk.maxwell.schema.ddl.ResolvedSchemaChange;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;
import snaq.db.ConnectionPool;


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

	private MysqlSavedSchema(Long serverID, CaseSensitivity sensitivity) throws SQLException {
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

		ResultSet rs = preparedStatement.getGeneratedKeys();

		if (rs.next()) {
			return rs.getLong(1);
		} else
			return null;
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
		} catch ( MySQLIntegrityConstraintViolationException e ) {
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
		PreparedStatement p = c.prepareStatement("SELECT * from `schemas` where position_sha = ?");
		p.setString(1, sha);
		ResultSet rs = p.executeQuery();

		if ( rs.next() ) {
			Long id = rs.getLong("id");
			LOGGER.debug("findSchemaForPositionSHA: found schema_id: " + id + " for sha: " + sha);
			return id;
		} else {
			return null;
		}
	}

	private Long saveDerivedSchema(Connection conn) throws SQLException {
		PreparedStatement insert = conn.prepareStatement(
				"INSERT into `schemas` SET base_schema_id = ?, deltas = ?, binlog_file = ?, " +
				"binlog_position = ?, server_id = ?, charset = ?, version = ?, " +
				"position_sha = ?, gtid_set = ?, last_heartbeat_read = ?",
				Statement.RETURN_GENERATED_KEYS);

		String deltaString;

		try {
			deltaString = mapper.writerFor(listOfResolvedSchemaChangeType).writeValueAsString(deltas);
		} catch ( JsonProcessingException e ) {
			throw new RuntimeException("Couldn't serialize " + deltas + " to JSON.");
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

	public Long saveSchema(Connection conn) throws SQLException {
		if ( this.baseSchemaID != null )
			return saveDerivedSchema(conn);

		PreparedStatement schemaInsert, databaseInsert, tableInsert;

		schemaInsert = conn.prepareStatement(
				"INSERT INTO `schemas` SET binlog_file = ?, binlog_position = ?, server_id = ?, charset = ?, version = ?, position_sha = ?, gtid_set = ?, last_heartbeat_read = ?",
				Statement.RETURN_GENERATED_KEYS
		);

		databaseInsert = conn.prepareStatement(
				"INSERT INTO `databases` SET schema_id = ?, name = ?, charset=?",
				Statement.RETURN_GENERATED_KEYS
		);

		tableInsert = conn.prepareStatement(
				"INSERT INTO `tables` SET schema_id = ?, database_id = ?, name = ?, charset=?, pk=?",
				Statement.RETURN_GENERATED_KEYS
		);

		BinlogPosition binlogPosition = position.getBinlogPosition();
		Long schemaId = executeInsert(schemaInsert, binlogPosition.getFile(),
				binlogPosition.getOffset(), serverID, schema.getCharset(), SchemaStoreVersion,
				getPositionSHA(), binlogPosition.getGtidSetStr(), position.getLastHeartbeatRead());

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

		return schemaId;
	}

	private void executeColumnInsert(Connection conn, ArrayList<Object> columnData) throws SQLException {
		String insertColumnSQL = this.columnInsertSQL;

		for (int i=1; i < columnData.size() / 8; i++) {
			insertColumnSQL = insertColumnSQL + ", (?, ?, ?, ?, ?, ?, ?, ?)";
		}

		PreparedStatement columnInsert = conn.prepareStatement(insertColumnSQL);
		int i = 1;

		for (Object o : columnData)
			columnInsert.setObject(i++,  o);

		columnInsert.execute();
		columnInsert.close();
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
			savedSchema.handleVersionUpgrades(conn);

			return savedSchema;
		}
	}

	public static MysqlSavedSchema restoreFromSchemaID(MysqlSavedSchema savedSchema, MaxwellContext context) throws SQLException, InvalidSchemaError {
		try ( Connection conn = context.getMaxwellConnectionPool().getConnection() ) {
			Long schemaID = savedSchema.getSchemaID();
			if (schemaID == null)
				return null;

			savedSchema.restoreFromSchemaID(conn, schemaID);
			return savedSchema;
		}
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

	private HashMap<Long, HashMap<String, Object>> buildSchemaMap(Connection conn) throws SQLException {
		HashMap<Long, HashMap<String, Object>> schemas = new HashMap<>();

		PreparedStatement p = conn.prepareStatement("SELECT * from `schemas`");
		ResultSet rs = p.executeQuery();

		ResultSetMetaData md = rs.getMetaData();
		while ( rs.next() ) {
			HashMap<String, Object> row = new HashMap<>();
			for ( int i = 1; i <= md.getColumnCount(); i++ )
				row.put(md.getColumnName(i), rs.getObject(i));
			schemas.put(rs.getLong("id"), row);
		}
		rs.close();
		return schemas;
	}

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
		PreparedStatement p = conn.prepareStatement("select * from `schemas` where id = " + schemaID);
		ResultSet schemaRS = p.executeQuery();

		schemaRS.next();

		setPosition(new Position(
			new BinlogPosition(
				schemaRS.getString("gtid_set"),
				null,
				schemaRS.getInt("binlog_position"),
				schemaRS.getString("binlog_file")
			), schemaRS.getLong("last_heartbeat_read")
		));

		LOGGER.info("Restoring schema id " + schemaRS.getInt("id") + " (last modified at " + this.position + ")");

		this.schemaID = schemaRS.getLong("id");
		this.baseSchemaID = schemaRS.getLong("base_schema_id");

		if ( schemaRS.wasNull() )
			this.baseSchemaID = null;

		this.deltas = parseDeltas(schemaRS.getString("deltas"));
		this.schemaVersion = schemaRS.getInt("version");
		this.schema = new Schema(new ArrayList<Database>(), schemaRS.getString("charset"), this.sensitivity);
	}


	private void restoreFullSchema(Connection conn, Long schemaID) throws SQLException, InvalidSchemaError {
		PreparedStatement p = conn.prepareStatement(
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
						"ORDER BY d.id, t.id, c.id"
		);

		p.setLong(1, this.schemaID);
		ResultSet rs = p.executeQuery();

		Database currentDatabase = null;
		Table currentTable = null;
		int columnIndex = 0;

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
				currentDatabase = new Database(dbName, dbCharset);
				this.schema.addDatabase(currentDatabase);
				// make sure two tables named the same in different dbs are picked up.
				currentTable = null;
				LOGGER.debug("Restoring database " + dbName + "...");
			}

			if (tName == null) {
				// if tName is null, there are no tables connected to this database
				continue;
			} else if (currentTable == null || !currentTable.getName().equals(tName)) {
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
				columnLength = Long.valueOf(columnLengthInt);
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
			currentTable.addColumn(c);

		}
		rs.close();
		LOGGER.debug("Restored all databases");
	}

	private static Long findSchema(Connection connection, Position targetPosition, Long serverID)
			throws SQLException {
		LOGGER.debug("looking to restore schema at target position " + targetPosition);
		BinlogPosition targetBinlogPosition = targetPosition.getBinlogPosition();
		if (targetBinlogPosition.getGtidSetStr() != null) {
			PreparedStatement s = connection.prepareStatement(
				"SELECT id, gtid_set from `schemas` "
				+ "WHERE deleted = 0 "
				+ "ORDER BY id desc");

			ResultSet rs = s.executeQuery();
			while (rs.next()) {
				Long id = rs.getLong("id");
				String gtid = rs.getString("gtid_set");
				LOGGER.debug("Retrieving schema at id: " + id + " gtid: " + gtid);
				if (gtid != null) {
					GtidSet gtidSet = new GtidSet(gtid);
					if (gtidSet.isContainedWithin(targetBinlogPosition.getGtidSet())) {
						LOGGER.debug("Found contained schema: " + id);
						return id;
					}
				}
			}
			return null;
		} else {
			// Only consider binlog positions before the target position on the current server.
			// Within those, sort for the latest binlog file, then the latest binlog position.
			PreparedStatement s = connection.prepareStatement(
				"SELECT id from `schemas` "
				+ "WHERE deleted = 0 "
				+ "AND last_heartbeat_read <= ? AND ((binlog_file < ?) OR (binlog_file = ? and binlog_position < ?)) AND server_id = ? "
				+ "ORDER BY last_heartbeat_read DESC, binlog_file DESC, binlog_position DESC limit 1");

			s.setLong(1, targetPosition.getLastHeartbeatRead());
			s.setString(2, targetBinlogPosition.getFile());
			s.setString(3, targetBinlogPosition.getFile());
			s.setLong(4, targetBinlogPosition.getOffset());
			s.setLong(5, serverID);

			ResultSet rs = s.executeQuery();
			if (rs.next()) {
				return rs.getLong("id");
			} else
				return null;
		}
	}

	public Schema getSchema() {
		return this.schema;
	}

	public void setSchema(Schema s) {
		this.schema = s;
	}

	private void ensureSchemaID() {
		if ( this.schemaID == null ) {
			throw new RuntimeException("Can't destroy uninitialized schema!");
		}
	}

	private void setPosition(Position position) {
		this.position = position;
	}

	public static void delete(Connection connection, long schema_id) throws SQLException {
		connection.createStatement().execute("update `schemas` set deleted = 1 where id = " + schema_id);
	}

	public void destroy(Connection connection) throws SQLException {
		ensureSchemaID();

		String[] tables = { "databases", "tables", "columns" };
		connection.createStatement().execute("delete from `schemas` where id = " + schemaID);
		for ( String tName : tables ) {
			connection.createStatement().execute("delete from `" + tName + "` where schema_id = " + schemaID);
		}
	}

	public boolean schemaExists(Connection connection, long schema_id) throws SQLException {
		if ( this.schemaID == null )
			return false;
		ResultSet rs = connection.createStatement().executeQuery("select id from `schemas` where id = " + schema_id);
		return rs.next();
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

		for ( Pair<ColumnDef, ColumnDef> pair : schema.matchColumns(recaptured) ) {
			ColumnDef cA = pair.getLeft();
			ColumnDef cB = pair.getRight();

			if (cA instanceof IntColumnDef) {
				if (cB != null && cB instanceof IntColumnDef) {
					if (((IntColumnDef) cA).isSigned() && !((IntColumnDef) cB).isSigned()) {
						((IntColumnDef) cA).setSigned(false);
						unsignedDiffs++;
					}
				} else {
					LOGGER.warn("warning: Couldn't check for unsigned integer bug on column " + cA.getName() +
						".  You may want to recapture your schema");
				}
			} else if (cA instanceof BigIntColumnDef) {
				if (cB != null && cB instanceof BigIntColumnDef) {
					if (((BigIntColumnDef) cA).isSigned() && !((BigIntColumnDef) cB).isSigned())
						((BigIntColumnDef) cA).setSigned(false);
					unsignedDiffs++;
				} else {
					LOGGER.warn("warning: Couldn't check for unsigned integer bug on column " + cA.getName() +
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

	private void fixColumnCases(Schema recaptured) throws SQLException {
		int caseDiffs = 0;

		for ( Pair<ColumnDef, ColumnDef> pair : schema.matchColumns(recaptured) ) {
			ColumnDef cA = pair.getLeft();
			ColumnDef cB = pair.getRight();

			if ( !cA.getName().equals(cB.getName()) ) {
				LOGGER.info("correcting column case of `" + cA.getName() + "` to `" + cB.getName() + "`.  Will save a full schema snapshot after the new DDL update is processed.");
				caseDiffs++;
				cA.setName(cB.getName());
			}
		}

		if ( caseDiffs > 0 )
			this.shouldSnapshotNextSchema = true;
	}

	private void fixColumnLength(Schema recaptured) throws SQLException {
		int colLengthDiffs = 0;

		for ( Pair<ColumnDef, ColumnDef> pair : schema.matchColumns(recaptured) ) {
			ColumnDef cA = pair.getLeft();
			ColumnDef cB = pair.getRight();

			if (cA instanceof ColumnDefWithLength) {
				if (cB != null && cB instanceof ColumnDefWithLength) {
					long aColLength = ((ColumnDefWithLength) cA).getColumnLength();
					long bColLength = ((ColumnDefWithLength) cB).getColumnLength();

					if ( aColLength != bColLength ) {
						colLengthDiffs++;
						LOGGER.info("correcting column length of `" + cA.getName() + "` to " + bColLength + ".  Will save a full schema snapshot after the new DDL update is processed.");
						((ColumnDefWithLength) cA).setColumnLength(bColLength);
					}
				} else {
					LOGGER.warn("warning: Couldn't check for column length on column " + cA.getName() +
						".  You may want to recapture your schema");
				}
			}

			if ( colLengthDiffs > 0 )
				this.shouldSnapshotNextSchema = true;
		}
	}

	protected void handleVersionUpgrades(Connection conn) throws SQLException, InvalidSchemaError {
		if ( this.schemaVersion < 3 ) {
			Schema recaptured = new SchemaCapturer(conn, sensitivity).capture();

			if ( this.schemaVersion < 1 ) {
				if ( this.schema != null && this.schema.findDatabase("mysql") == null ) {
					LOGGER.info("Could not find mysql db, adding it to schema");
					SchemaCapturer sc = new SchemaCapturer(conn, sensitivity, "mysql");
					Database db = sc.capture().findDatabase("mysql");
					this.schema.addDatabase(db);
					this.shouldSnapshotNextSchema = true;
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
}
