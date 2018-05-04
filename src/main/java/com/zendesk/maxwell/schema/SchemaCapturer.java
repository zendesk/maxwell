package com.zendesk.maxwell.schema;

import com.zendesk.maxwell.CaseSensitivity;
import com.zendesk.maxwell.schema.columndef.ColumnDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SchemaCapturer {
	private final Connection connection;
	static final Logger LOGGER = LoggerFactory.getLogger(MysqlSavedSchema.class);

	public static final HashSet<String> IGNORED_DATABASES = new HashSet<String>(
			Arrays.asList(new String[]{"performance_schema", "information_schema"})
	);

	private final HashSet<String> includeDatabases;

	private final CaseSensitivity sensitivity;

	private PreparedStatement tablePreparedStatement;

	private PreparedStatement columnPreparedStatement;

	private PreparedStatement pkPreparedStatement;

	public SchemaCapturer(Connection c, CaseSensitivity sensitivity) throws SQLException {
		this.includeDatabases = new HashSet<>();
		this.connection = c;
		this.sensitivity = sensitivity;

		String tblSql = "SELECT TABLES.TABLE_NAME, CCSA.CHARACTER_SET_NAME "
				+ "FROM INFORMATION_SCHEMA.TABLES "
				+ "JOIN information_schema.COLLATION_CHARACTER_SET_APPLICABILITY AS CCSA"
				+ " ON TABLES.TABLE_COLLATION = CCSA.COLLATION_NAME WHERE TABLES.TABLE_SCHEMA = ?";

		tablePreparedStatement = connection.prepareStatement(tblSql);

		String dateTimePrecision = "";
		if(isMySQLAtLeast56())
			dateTimePrecision = "DATETIME_PRECISION, ";

		String columnSql = "SELECT " +
				"TABLE_NAME," +
				"COLUMN_NAME, " +
				"DATA_TYPE, " +
				"CHARACTER_SET_NAME, " +
				"ORDINAL_POSITION, " +
				"COLUMN_TYPE, " +
				dateTimePrecision +
				"COLUMN_KEY, " +
				"COLUMN_TYPE " +
				"FROM `information_schema`.`COLUMNS` WHERE TABLE_SCHEMA = ?";

		columnPreparedStatement = connection.prepareStatement(columnSql);

		String pkSql = "SELECT TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION FROM information_schema.KEY_COLUMN_USAGE "
				+ "WHERE CONSTRAINT_NAME = 'PRIMARY' AND TABLE_SCHEMA = ?";

		pkPreparedStatement = connection.prepareStatement(pkSql);

	}

	public SchemaCapturer(Connection c, CaseSensitivity sensitivity, String dbName) throws SQLException {
		this(c, sensitivity);
		this.includeDatabases.add(dbName);
	}

	public Schema capture() throws SQLException {
		LOGGER.debug("Capturing schemas...");
		ArrayList<Database> databases = new ArrayList<>();

		ResultSet rs = connection.createStatement().executeQuery(
				"SELECT SCHEMA_NAME, DEFAULT_CHARACTER_SET_NAME FROM INFORMATION_SCHEMA.SCHEMATA"
		);
		while (rs.next()) {
			String dbName = rs.getString("SCHEMA_NAME");
			String charset = rs.getString("DEFAULT_CHARACTER_SET_NAME");

			if (includeDatabases.size() > 0 && !includeDatabases.contains(dbName))
				continue;

			if (IGNORED_DATABASES.contains(dbName))
				continue;

			Database db = new Database(dbName, charset);
			databases.add(db);
		}
		rs.close();

		int size = databases.size();
		LOGGER.debug("Starting schema capture of " + size + " databases...");
		int counter = 1;
		for (Database db : databases) {
			LOGGER.debug(counter + "/" + size + " Capturing " + db.getName() + "...");
			captureDatabase(db);
			counter++;
		}
		LOGGER.debug(size + " database schemas captured!");

		return new Schema(databases, captureDefaultCharset(), this.sensitivity);
	}

	private String captureDefaultCharset() throws SQLException {
		LOGGER.debug("Capturing Default Charset");
		ResultSet rs = connection.createStatement().executeQuery("select @@character_set_server");
		rs.next();
		return rs.getString("@@character_set_server");
	}


	private void captureDatabase(Database db) throws SQLException {
		tablePreparedStatement.setString(1, db.getName());
		ResultSet rs = tablePreparedStatement.executeQuery();

		HashMap<String, Table> tables = new HashMap<>();
		while (rs.next()) {
			String tableName = rs.getString("TABLE_NAME");
			String characterSetName = rs.getString("CHARACTER_SET_NAME");
			Table t = db.buildTable(tableName, characterSetName);
			tables.put(tableName, t);
		}
		rs.close();

		captureTables(db, tables);
	}


	private boolean isMySQLAtLeast56() throws SQLException {
		java.sql.DatabaseMetaData meta = connection.getMetaData();
		int major = meta.getDatabaseMajorVersion();
		int minor = meta.getDatabaseMinorVersion();
		return ((major == 5 && minor >= 6) || major > 5);
	}


	private void captureTables(Database db, HashMap<String, Table> tables) throws SQLException {

		columnPreparedStatement.setString(1, db.getName());
		ResultSet r = columnPreparedStatement.executeQuery();

		boolean hasDatetimePrecision = isMySQLAtLeast56();

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
				int colPos = r.getInt("ORDINAL_POSITION") - 1;
				boolean colSigned = !r.getString("COLUMN_TYPE").matches(".* unsigned$");
				Long columnLength = null;

				if (hasDatetimePrecision)
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
		r.close();

		captureTablesPK(db, tables);
	}

	private void captureTablesPK(Database db, HashMap<String, Table> tables) throws SQLException {
		pkPreparedStatement.setString(1, db.getName());
		ResultSet rs = pkPreparedStatement.executeQuery();

		HashMap<String, ArrayList<String>> tablePKMap = new HashMap<>();

		for (String tableName : tables.keySet()) {
			tablePKMap.put(tableName, new ArrayList<String>());
		}

		while (rs.next()) {
			int ordinalPosition = rs.getInt("ORDINAL_POSITION");
			String tableName = rs.getString("TABLE_NAME");
			String columnName = rs.getString("COLUMN_NAME");

			ArrayList<String> pkList = tablePKMap.get(tableName);
			if ( pkList != null )
				pkList.add(ordinalPosition - 1, columnName);
		}
		rs.close();

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

}
