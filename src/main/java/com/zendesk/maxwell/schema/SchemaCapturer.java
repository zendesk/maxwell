package com.zendesk.maxwell.schema;

import com.zendesk.maxwell.CaseSensitivity;
import com.zendesk.maxwell.schema.columndef.ColumnDef;
import com.zendesk.maxwell.schema.columndef.JsonColumnDef;
import com.zendesk.maxwell.schema.columndef.StringColumnDef;
import com.zendesk.maxwell.schema.ddl.InvalidSchemaError;
import com.zendesk.maxwell.util.Sql;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SchemaCapturer implements AutoCloseable {
	private final Connection connection;
	static final Logger LOGGER = LoggerFactory.getLogger(SchemaCapturer.class);

	public static final HashSet<String> IGNORED_DATABASES = new HashSet<>(
			Arrays.asList(new String[]{"performance_schema", "information_schema"})
	);

	private final Set<String> includeDatabases;
	private final Set<String> includeTables;

	private final CaseSensitivity sensitivity;
	private final boolean isMySQLAtLeast56;
	private final PreparedStatement tablePreparedStatement;
	private final PreparedStatement columnPreparedStatement;
	private final PreparedStatement pkPreparedStatement;


	public SchemaCapturer(Connection c, CaseSensitivity sensitivity) throws SQLException {
		this(c, sensitivity, Collections.emptySet(), Collections.emptySet());
	}

	SchemaCapturer(Connection c, CaseSensitivity sensitivity, Set<String> includeDatabases, Set<String> includeTables) throws SQLException {
		this.includeDatabases = includeDatabases;
		this.includeTables = includeTables;
		this.connection = c;
		this.sensitivity = sensitivity;

		this.isMySQLAtLeast56 = isMySQLAtLeast56();
		String dateTimePrecision = "";
		if (isMySQLAtLeast56) {
			dateTimePrecision = "DATETIME_PRECISION, ";
		}

		String tblSql = "SELECT TABLES.TABLE_NAME, CCSA.CHARACTER_SET_NAME "
				+ "FROM INFORMATION_SCHEMA.TABLES "
				+ "JOIN information_schema.COLLATION_CHARACTER_SET_APPLICABILITY AS CCSA"
				+ " ON TABLES.TABLE_COLLATION = CCSA.COLLATION_NAME WHERE TABLES.TABLE_SCHEMA = ? ";

		if (!includeTables.isEmpty()) {
			tblSql += " AND TABLES.TABLE_NAME IN " + Sql.inListSQL(includeTables.size());
		}
		tablePreparedStatement = connection.prepareStatement(tblSql);

		String columnSql = "SELECT " +
				"TABLE_NAME," +
				"COLUMN_NAME, " +
				"DATA_TYPE, " +
				"CHARACTER_SET_NAME, " +
				"ORDINAL_POSITION, " +
				"COLUMN_TYPE, " +
				dateTimePrecision +
				"COLUMN_KEY " +
				"FROM `information_schema`.`COLUMNS` WHERE TABLE_SCHEMA = ? ORDER BY TABLE_NAME, ORDINAL_POSITION";

		columnPreparedStatement = connection.prepareStatement(columnSql);

		String pkSQl = "SELECT " +
				"TABLE_NAME, " +
				"COLUMN_NAME, " +
				"ORDINAL_POSITION " +
				"FROM information_schema.KEY_COLUMN_USAGE " +
				"WHERE CONSTRAINT_NAME = 'PRIMARY' AND TABLE_SCHEMA = ? " +
				"ORDER BY TABLE_NAME, ORDINAL_POSITION";

		pkPreparedStatement = connection.prepareStatement(pkSQl);
	}

	public SchemaCapturer(Connection c, CaseSensitivity sensitivity, String dbName) throws SQLException {
		this(c, sensitivity, Collections.singleton(dbName), Collections.emptySet());
	}

	public SchemaCapturer(Connection c, CaseSensitivity sensitivity, String dbName, String tblName) throws SQLException {
		this(c, sensitivity, Collections.singleton(dbName), Collections.singleton(tblName));
	}

	public Schema capture() throws SQLException {
		LOGGER.debug("Capturing schemas...");
		ArrayList<Database> databases = new ArrayList<>();

		String dbCaptureQuery =
			"SELECT SCHEMA_NAME, DEFAULT_CHARACTER_SET_NAME FROM INFORMATION_SCHEMA.SCHEMATA";

		if ( includeDatabases.size() > 0 ) {
			dbCaptureQuery +=
				" WHERE SCHEMA_NAME IN " + Sql.inListSQL(includeDatabases.size());
		}
		dbCaptureQuery += " ORDER BY SCHEMA_NAME";

		try (PreparedStatement statement = connection.prepareStatement(dbCaptureQuery)) {
			Sql.prepareInList(statement, 1, includeDatabases);

			try (ResultSet rs = statement.executeQuery()) {
				while (rs.next()) {
					String dbName = rs.getString("SCHEMA_NAME");
					String charset = rs.getString("DEFAULT_CHARACTER_SET_NAME");

					if (IGNORED_DATABASES.contains(dbName))
						continue;

					Database db = new Database(dbName, charset);
					databases.add(db);
				}
			}
		}

		int size = databases.size();
		LOGGER.debug("Starting schema capture of {} databases...", size);
		int counter = 1;
		for (Database db : databases) {
			LOGGER.debug("{}/{} Capturing {}...", counter, size, db.getName());
			captureDatabase(db);
			counter++;
		}
		LOGGER.debug("{} database schemas captured!", size);


		Schema s = new Schema(databases, captureDefaultCharset(), this.sensitivity);
		try {
			if ( isMariaDB() )
				detectMariaDBJSON(s);
		} catch ( InvalidSchemaError e ) {
			e.printStackTrace();
		}
		return s;
	}

	private String captureDefaultCharset() throws SQLException {
		LOGGER.debug("Capturing Default Charset");
		try (Statement stmt = connection.createStatement();
			 ResultSet rs = stmt.executeQuery("select @@character_set_server")) {
			rs.next();
			return rs.getString("@@character_set_server");
		}
	}


	private void captureDatabase(Database db) throws SQLException {
		tablePreparedStatement.setString(1, db.getName());
		Sql.prepareInList(tablePreparedStatement, 2, includeTables);

		HashMap<String, Table> tables = new HashMap<>();
		try (ResultSet rs = tablePreparedStatement.executeQuery()) {
			while (rs.next()) {
				String tableName = rs.getString("TABLE_NAME");
				String characterSetName = rs.getString("CHARACTER_SET_NAME");
				Table t = db.buildTable(tableName, characterSetName);
				tables.put(tableName, t);
			}
		}
		captureTables(db, tables);
	}


	private boolean isMySQLAtLeast56() throws SQLException {
		if ( isMariaDB() )
			return true;

		java.sql.DatabaseMetaData meta = connection.getMetaData();
		int major = meta.getDatabaseMajorVersion();
		int minor = meta.getDatabaseMinorVersion();
		return ((major == 5 && minor >= 6) || major > 5);
	}

	private boolean isMariaDB() throws SQLException {
		java.sql.DatabaseMetaData meta = connection.getMetaData();
		return meta.getDatabaseProductVersion().toLowerCase().contains("maria");
	}

	private void captureTables(Database db, HashMap<String, Table> tables) throws SQLException {
		columnPreparedStatement.setString(1, db.getName());

		try (ResultSet r = columnPreparedStatement.executeQuery()) {

			HashMap<String, Integer> pkIndexCounters = new HashMap<>();
			for (String tableName : tables.keySet()) {
				pkIndexCounters.put(tableName, 0);
			}

			while (r.next()) {
				String[] enumValues = null;
				String tableName = r.getString("TABLE_NAME");

				if (tables.containsKey(tableName)) {
					Table t = tables.get(tableName);
					String colName = r.getString("COLUMN_NAME");
					String colType = r.getString("DATA_TYPE");
					String colEnc = r.getString("CHARACTER_SET_NAME");
					short colPos = (short) (r.getInt("ORDINAL_POSITION") - 1);
					boolean colSigned = !r.getString("COLUMN_TYPE").matches(".* unsigned$");
					Long columnLength = null;

					if (isMySQLAtLeast56)
						columnLength = r.getLong("DATETIME_PRECISION");

					if (r.getString("COLUMN_KEY").equals("PRI"))
						t.pkIndex = pkIndexCounters.get(tableName);

					if (colType.equals("enum") || colType.equals("set")) {
						String expandedType = r.getString("COLUMN_TYPE");

						enumValues = extractEnumValues(expandedType);
					}

					t.addColumn(ColumnDef.build(colName, colEnc, colType, colPos, colSigned, enumValues, columnLength));

					pkIndexCounters.put(tableName, pkIndexCounters.get(tableName) + 1);
				}
			}
		}

		captureTablesPK(db, tables);
	}

	private void captureTablesPK(Database db, HashMap<String, Table> tables) throws SQLException {
		pkPreparedStatement.setString(1, db.getName());

		HashMap<String, ArrayList<String>> tablePKMap = new HashMap<>();

		try (ResultSet rs = pkPreparedStatement.executeQuery()) {
			for (String tableName : tables.keySet()) {
				tablePKMap.put(tableName, new ArrayList<>());
			}

			while (rs.next()) {
				int ordinalPosition = rs.getInt("ORDINAL_POSITION");
				String tableName = rs.getString("TABLE_NAME");
				String columnName = rs.getString("COLUMN_NAME");

				ArrayList<String> pkList = tablePKMap.get(tableName);
				if ( pkList != null )
					pkList.add(ordinalPosition - 1, columnName);
			}
		}

		for (Map.Entry<String, Table> entry : tables.entrySet()) {
			String key = entry.getKey();
			Table table = entry.getValue();

			table.setPKList(tablePKMap.get(key));
		}
	}

	static String[] extractEnumValues(String expandedType) {
		Matcher matcher = Pattern.compile("(enum|set)\\((.*)\\)").matcher(expandedType);
		matcher.matches(); // why do you tease me so.
		String enumValues = matcher.group(2);

		if (!(enumValues.endsWith(","))) {
			enumValues += ",";
		}

		String regex = "('.*?'),";
		Pattern pattern = Pattern.compile(regex);
		Matcher enumMatcher = pattern.matcher(enumValues);

		List<String> result = new ArrayList<>();
		while(enumMatcher.find()) {
			String value = enumMatcher.group(0);
			if (value.startsWith("'"))
				value = value.substring(1);
			if (value.endsWith("',")) {
				value = value.substring(0, value.length() - 2);
			}
			result.add(value);
		}
		return result.toArray(new String[0]);
	}

	@Override
	public void close() throws SQLException {
		try (PreparedStatement p1 = tablePreparedStatement;
			 PreparedStatement p2 = columnPreparedStatement;
			 PreparedStatement p3 = pkPreparedStatement) {
			// auto-close shared prepared statements
		}
	}

	private void detectMariaDBJSON(Schema schema) throws SQLException, InvalidSchemaError {
		String checkConstraintSQL = "SELECT CONSTRAINT_SCHEMA, TABLE_NAME, CONSTRAINT_NAME, CHECK_CLAUSE " +
			"from INFORMATION_SCHEMA.CHECK_CONSTRAINTS " +
			"where LEVEL='column' and CHECK_CLAUSE LIKE 'json_valid(%)'";

		String regex = "json_valid\\(`(.*)`\\)";
		Pattern pattern = Pattern.compile(regex);

		try (
			PreparedStatement statement = connection.prepareStatement(checkConstraintSQL);
			ResultSet rs = statement.executeQuery()
		) {
			while ( rs.next() ) {
				String checkClause = rs.getString("CHECK_CLAUSE");
				Matcher m = pattern.matcher(checkClause);
				if ( m.find() ) {
					String column = m.group(1);
					Database d = schema.findDatabase(rs.getString("CONSTRAINT_SCHEMA"));
					if ( d == null ) continue;
					Table t = d.findTable(rs.getString("TABLE_NAME"));
					if ( t == null ) continue;
					short i = t.findColumnIndex(column);
					if ( i < 0 ) continue;

					ColumnDef cd = t.findColumn(i);
					if ( cd instanceof StringColumnDef ) {
						t.replaceColumn(i, JsonColumnDef.create(cd.getName(), "json", i));
					}
				}
			}
		}

	}

}
