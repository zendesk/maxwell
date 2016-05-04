package com.zendesk.maxwell.schema;

import java.sql.*;
import java.util.*;

import java.io.IOException;

import com.fasterxml.jackson.databind.JavaType;
import com.zendesk.maxwell.CaseSensitivity;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.schema.columndef.*;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zendesk.maxwell.BinlogPosition;
import com.zendesk.maxwell.schema.ddl.InvalidSchemaError;
import com.zendesk.maxwell.schema.ddl.ResolvedSchemaChange;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;

public class SchemaStore {
	static int SchemaStoreVersion = 1;

	private Schema schema;
	private BinlogPosition position;
	private Long schema_id;
	private int schemaVersion;

	private Long base_schema_id;
	private List<ResolvedSchemaChange> deltas;

	private static final ObjectMapper mapper = new ObjectMapper();
	private static final JavaType listOfResolvedSchemaChangeType = mapper.getTypeFactory().constructCollectionType(List.class, ResolvedSchemaChange.class);

	static final Logger LOGGER = LoggerFactory.getLogger(SchemaStore.class);

	private final static String columnInsertSQL =
		"INSERT INTO `columns` (schema_id, table_id, name, charset, coltype, is_signed, enum_values) VALUES (?, ?, ?, ?, ?, ?, ?)";

	private final CaseSensitivity sensitivity;
	private final Long serverID;

	private boolean shouldSnapshotNextSchema = false;

	public SchemaStore(Long serverID, CaseSensitivity sensitivity) throws SQLException {
		this.serverID = serverID;
		this.sensitivity = sensitivity;
	}

	public SchemaStore(Long serverID, CaseSensitivity sensitivity, Schema schema, BinlogPosition position) throws SQLException {
		this(serverID, sensitivity);
		this.schema = schema;
		this.position = position;
	}

	public SchemaStore(MaxwellContext context, Schema schema, BinlogPosition position) throws SQLException {
		this(context.getServerID(), context.getCaseSensitivity(), schema, position);
	}

	public SchemaStore(Long serverID, CaseSensitivity sensitivity, Schema schema, BinlogPosition position,
			long base_schema_id, List<ResolvedSchemaChange> deltas) throws SQLException {
		this(serverID, sensitivity);

		this.schema = schema;
		this.base_schema_id = base_schema_id;
		this.deltas = deltas;

		this.position = position;
	}

	public SchemaStore createDerivedSchema(Schema newSchema, BinlogPosition position, List<ResolvedSchemaChange> deltas) throws SQLException {
		if ( this.shouldSnapshotNextSchema )
			return new SchemaStore(this.serverID, this.sensitivity, newSchema, position);
		else
			return new SchemaStore(this.serverID, this.sensitivity, newSchema, position, this.schema_id, deltas);
	}

	public Long getSchemaID() {
		return schema_id;
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

	public void save(Connection connection) throws SQLException {
		if (this.schema == null)
			throw new RuntimeException("Uninitialized schema!");

		try {
			connection.setAutoCommit(false);
			this.schema_id = saveSchema(connection);
			connection.commit();
		} finally {
			connection.setAutoCommit(true);
		}
	}

	public Long saveDerivedSchema(Connection conn) throws SQLException {
		PreparedStatement insert = conn.prepareStatement(
				"INSERT into `schemas` SET base_schema_id = ?, deltas = ?, binlog_file = ?, binlog_position = ?, server_id = ?, charset = ?, version = ?",
				Statement.RETURN_GENERATED_KEYS);

		String deltaString;

		try {
			deltaString = mapper.writerFor(listOfResolvedSchemaChangeType).writeValueAsString(deltas);
		} catch ( JsonProcessingException e ) {
			throw new RuntimeException("Couldn't serialize " + deltas + " to JSON.");
		}

		return executeInsert(insert,
		                     this.base_schema_id,
		                     deltaString,
		                     position.getFile(),
		                     position.getOffset(),
		                     serverID,
		                     schema.getCharset(),
		                     SchemaStoreVersion);

	}

	public Long saveSchema(Connection conn) throws SQLException {
		if ( this.base_schema_id != null )
			return saveDerivedSchema(conn);

		PreparedStatement schemaInsert, databaseInsert, tableInsert;

		schemaInsert = conn.prepareStatement(
				"INSERT INTO `schemas` SET binlog_file = ?, binlog_position = ?, server_id = ?, charset = ?, version = ?",
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

		Long schemaId = executeInsert(schemaInsert, position.getFile(),
				position.getOffset(), serverID, schema.getCharset(), SchemaStoreVersion);

		ArrayList<Object> columnData = new ArrayList<Object>();

		for (Database d : schema.getDatabases()) {
			Long dbId = executeInsert(databaseInsert, schemaId, d.getName(), d.getCharset());

			for (Table t : d.getTableList()) {
				Long tableId = executeInsert(tableInsert, schemaId, dbId, t.getName(), t.getCharset(), t.getPKString());


				for (ColumnDef c : t.getColumnList()) {
					String enumValuesSQL = null;

					if ( c instanceof EnumeratedColumnDef ) {
						EnumeratedColumnDef enumColumn = (EnumeratedColumnDef) c;
						enumValuesSQL = StringUtils.join(enumColumn.getEnumValues(), ",");
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

		for (int i=1; i < columnData.size() / 7; i++) {
			insertColumnSQL = insertColumnSQL + ", (?, ?, ?, ?, ?, ?, ?)";
		}

		PreparedStatement columnInsert = conn.prepareStatement(insertColumnSQL);
		int i = 1;

		for (Object o : columnData)
			columnInsert.setObject(i++,  o);

		columnInsert.execute();
		columnData.clear();
	}

	public static SchemaStore restore(Connection connection, MaxwellContext context) throws SQLException, IOException, InvalidSchemaError {
		SchemaStore s = new SchemaStore(context.getServerID(), context.getCaseSensitivity());

		s.restoreFrom(connection, context.getInitialPosition());

		return s;
	}

	private List<ResolvedSchemaChange> parseDeltas(String json) throws IOException {
		if ( json == null )
			return null;

		return mapper.readerFor(listOfResolvedSchemaChangeType).readValue(json.getBytes());
	}

	private HashMap<Long, HashMap<String, Object>> buildSchemaMap(Connection conn) throws SQLException {
		HashMap<Long, HashMap<String, Object>> schemas = new HashMap<>();

		PreparedStatement p = conn.prepareStatement("SELECT * from `schemas` where server_id = ?");
		p.setLong(1, this.serverID);
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

	private void restoreDerivedSchema(Connection conn, Long schema_id) throws SQLException, IOException, InvalidSchemaError {
		/* build hashmap of schema_id -> schema properties (as hash) */
		HashMap<Long, HashMap<String, Object>> schemas = buildSchemaMap(conn);

		/* walk backwards to build linked list with base schema at the
		 * head, and the rest of the delta schemas following */
		LinkedList<Long> schemaChain = buildSchemaChain(schemas, schema_id);

		Long firstSchemaId = schemaChain.removeFirst();

		/* do the "full" restore */
		SchemaStore firstSchema = new SchemaStore(serverID, sensitivity);
		firstSchema.restoreFromSchemaID(conn, firstSchemaId);
		Schema schema = firstSchema.getSchema();

		LOGGER.info("beginning to play deltas...");
		int count = 0;
		long startTime = System.currentTimeMillis();

		/* now walk the chain and play each schema's delta */
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

	private void restoreFrom(Connection conn, BinlogPosition targetPosition) throws SQLException, IOException, InvalidSchemaError {
		Long schemaID = findSchema(conn, targetPosition, this.serverID);
		if ( schemaID == null ) {
			throw new InvalidSchemaError("Could not find schema for "
					+ targetPosition.getFile() + ":" + targetPosition.getOffset());
		}

		restoreFromSchemaID(conn, schemaID);

		handleVersionUpgrades(conn, schemaID, this.schemaVersion);
	}

	private void restoreFromSchemaID(Connection conn, Long schemaID) throws SQLException, IOException, InvalidSchemaError {
		restoreSchemaMetadata(conn, schemaID);

		if ( this.base_schema_id != null )
			restoreDerivedSchema(conn, schemaID);
		else
			restoreFullSchema(conn, schemaID);
	}

	private void restoreSchemaMetadata(Connection conn, Long schemaID) throws SQLException, IOException {
		PreparedStatement p = conn.prepareStatement("select * from `schemas` where id = " + schemaID);
		ResultSet schemaRS = p.executeQuery();

		schemaRS.next();

		this.position = new BinlogPosition(schemaRS.getInt("binlog_position"), schemaRS.getString("binlog_file"));

		LOGGER.info("Restoring schema id " + schemaRS.getInt("id") + " (last modified at " + this.position + ")");

		this.schema_id = schemaRS.getLong("id");
		this.base_schema_id = schemaRS.getLong("base_schema_id");

		if ( schemaRS.wasNull() )
			this.base_schema_id = null;

		this.deltas = parseDeltas(schemaRS.getString("deltas"));
		this.schemaVersion = schemaRS.getInt("version");
		this.schema = new Schema(new ArrayList<Database>(), schemaRS.getString("charset"), this.sensitivity);
	}

	private void restoreFullSchema(Connection conn, Long schemaID) throws SQLException, IOException, InvalidSchemaError {
		PreparedStatement p = conn.prepareStatement("SELECT * from `databases` where schema_id = ? ORDER by id");
		p.setLong(1, this.schema_id);

		ResultSet dbRS = p.executeQuery();

		while (dbRS.next()) {
			this.schema.addDatabase(restoreDatabase(conn, dbRS.getInt("id"), dbRS.getString("name"), dbRS.getString("charset")));
		}

	}

	private Database restoreDatabase(Connection conn, int id, String name, String charset) throws SQLException {
		Statement s = conn.createStatement();
		Database d = new Database(name, charset);

		ResultSet tRS = s.executeQuery("SELECT * from `tables` where database_id = " + id + " ORDER by id");

		while (tRS.next()) {
			String tName = tRS.getString("name");
			String tCharset = tRS.getString("charset");
			String tPKs = tRS.getString("pk");

			int tID = tRS.getInt("id");

			restoreTable(conn, d, tName, tID, tCharset, tPKs);
		}
		return d;
	}

	private void restoreTable(Connection connection, Database d, String name, int id, String charset, String pks) throws SQLException {
		Statement s = connection.createStatement();

		Table t = d.buildTable(name, charset);

		ResultSet cRS = s.executeQuery("SELECT * from `columns` where table_id = " + id + " ORDER by id");

		int i = 0;
		while (cRS.next()) {
			String[] enumValues = null;
			if ( cRS.getString("enum_values") != null )
				enumValues = StringUtils.splitByWholeSeparatorPreserveAllTokens(cRS.getString("enum_values"), ",");

			ColumnDef c = ColumnDef.build(
					cRS.getString("name"), cRS.getString("charset"),
					cRS.getString("coltype"), i++,
					cRS.getInt("is_signed") == 1,
					enumValues);
			t.addColumn(c);
		}

		if ( pks != null ) {
			List<String> pkList = Arrays.asList(StringUtils.split(pks, ','));
			t.setPKList(pkList);
		}

	}

	private Long findSchema(Connection connection, BinlogPosition targetPosition, Long serverID)
			throws SQLException {
		LOGGER.debug("looking to restore schema at target position " + targetPosition);
		PreparedStatement s = connection.prepareStatement(
			"SELECT id from `schemas` "
			+ "WHERE deleted = 0 "
			+ "AND ((binlog_file < ?) OR (binlog_file = ? and binlog_position <= ?)) AND server_id = ? "
			+ "ORDER BY id desc limit 1");

		s.setString(1, targetPosition.getFile());
		s.setString(2, targetPosition.getFile());
		s.setLong(3, targetPosition.getOffset());
		s.setLong(4, serverID);

		ResultSet rs = s.executeQuery();
		if (rs.next()) {
			return rs.getLong("id");
		} else
			return null;
	}

	public Schema getSchema() {
		return this.schema;
	}

	public void setSchema(Schema s) {
		this.schema = s;
	}

	private void ensureSchemaID() {
		if ( this.schema_id == null ) {
			throw new RuntimeException("Can't destroy uninitialized schema!");
		}
	}

	public static void delete(Connection connection, long schema_id) throws SQLException {
		connection.createStatement().execute("update `schemas` set deleted = 1 where id = " + schema_id);
	}

	public void destroy(Connection connection) throws SQLException {
		ensureSchemaID();

		String[] tables = { "databases", "tables", "columns" };
		connection.createStatement().execute("delete from `schemas` where id = " + schema_id);
		for ( String tName : tables ) {
			connection.createStatement().execute("delete from `" + tName + "` where schema_id = " + schema_id);
		}
	}

	public boolean schemaExists(Connection connection, long schema_id) throws SQLException {
		if ( this.schema_id == null )
			return false;
		ResultSet rs = connection.createStatement().executeQuery("select id from `schemas` where id = " + schema_id);
		return rs.next();
	}

	public BinlogPosition getBinlogPosition() {
		return this.position;
	}

	private void fixUnsignedColumns(Connection conn) throws SQLException, InvalidSchemaError {
		int unsignedDiffs = 0;

		Schema recaptured = new SchemaCapturer(conn, sensitivity).capture();

		for ( Database dA : schema.getDatabases() ) {
			Database dB = recaptured.findDatabaseOrThrow(dA.getName());
			for ( Table tA : dA.getTableList() ) {
				Table tB = dB.findTableOrThrow(tA.getName());
				for ( ColumnDef cA : tA.getColumnList() ) {
					ColumnDef cB = tB.findColumnOrThrow(cA.getName());

					if ( cA instanceof IntColumnDef ) {
						if ( !(cB instanceof IntColumnDef) )
							throw new InvalidSchemaError("Expected " + cB.getName() + " to be an IntColumnDef");

						if ( ((IntColumnDef)cA).isSigned() && !((IntColumnDef)cB).isSigned() ) {
							((IntColumnDef)cA).setSigned(false);
							unsignedDiffs++;
						}
					} else if ( cA instanceof BigIntColumnDef ) {
						if ( !(cB instanceof BigIntColumnDef) )
							throw new InvalidSchemaError("Expected " + cB.getName() + " to be an BigIntColumnDef");

						if ( ((BigIntColumnDef)cA).isSigned() && !((BigIntColumnDef)cB).isSigned() )
							((BigIntColumnDef)cA).setSigned(false);
							unsignedDiffs++;

					}
				}
			}
		}

		if ( unsignedDiffs > 0 ) {
			/* A little explanation here: we've detected differences in signed-ness between the restored
			 * and the recaptured schema.  99.9% of the time this will be the result of our capture bug.
			 *
			 * We can't however simply re-save the re-captured schema, as we might be behind some DDL updates
			 * that we'd otherwise lose.  So we leave a marker so that the next time we save the schema, we'll
			 * purposely break the delta chain and fix the unsigned columns in the database.
			 * */
			this.shouldSnapshotNextSchema = true;
		}
	}

	private void handleVersionUpgrades(Connection conn, Long schemaID, int version) throws SQLException, InvalidSchemaError {
		if ( version < 1 ) {
			if ( this.schema != null && this.schema.findDatabase("mysql") == null ) {
				LOGGER.info("Could not find mysql db, adding it to schema");
				SchemaCapturer sc = new SchemaCapturer(conn, sensitivity, "mysql");
				Database db = sc.capture().findDatabase("mysql");
				this.schema.addDatabase(db);
				this.shouldSnapshotNextSchema = true;
			}

			fixUnsignedColumns(conn);
		}
	}
}
