package com.zendesk.maxwell.schema;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zendesk.maxwell.schema.columndef.ColumnDef;
import com.zendesk.maxwell.schema.ddl.SchemaSyncError;

public class SchemaCapturer {
	private final Connection connection;
	static final Logger LOGGER = LoggerFactory.getLogger(SchemaStore.class);

	public static final HashSet<String> IGNORED_DATABASES = new HashSet<String>(
		Arrays.asList(new String[] {"performance_schema", "information_schema"})
	);

	private final HashSet<String> includeDatabases;

	private final PreparedStatement infoSchemaStmt;
	private final String INFORMATION_SCHEMA_SQL =
			"SELECT * FROM `information_schema`.`COLUMNS` WHERE `TABLE_SCHEMA` = ? AND `TABLE_NAME` = ?";

	public SchemaCapturer(Connection c) throws SQLException {
		this.includeDatabases = new HashSet<String>();
		this.connection = c;
		this.infoSchemaStmt = connection.prepareStatement(INFORMATION_SCHEMA_SQL);
	}

	public SchemaCapturer(Connection c, String dbName) throws SQLException {
		this(c);
		this.includeDatabases.add(dbName);
	}

	public Schema capture() throws SQLException, SchemaSyncError {
		LOGGER.debug("Capturing schema");
		ArrayList<Database> databases = new ArrayList<Database>();


		ResultSet rs = connection.createStatement().executeQuery("SELECT * from INFORMATION_SCHEMA.SCHEMATA");

		while ( rs.next() ) {
			String dbName = rs.getString("SCHEMA_NAME");
			String encoding = rs.getString("DEFAULT_CHARACTER_SET_NAME");

			if ( includeDatabases.size() > 0 && !includeDatabases.contains(dbName))
				continue;

			if ( IGNORED_DATABASES.contains(dbName) )
				continue;

			databases.add(captureDatabase(dbName, encoding));
		}

		LOGGER.debug("Finished capturing schema");
		return new Schema(databases, captureDefaultEncoding());
	}

	private String captureDefaultEncoding() throws SQLException {
		ResultSet rs = connection.createStatement().executeQuery("select @@character_set_server");
		rs.next();
		return rs.getString("@@character_set_server");
	}

	private static final String tblSQL =
			  "SELECT TABLES.TABLE_NAME, CCSA.CHARACTER_SET_NAME "
			+ "FROM INFORMATION_SCHEMA.TABLES "
			+ "JOIN  information_schema.COLLATION_CHARACTER_SET_APPLICABILITY AS CCSA"
			+ " ON TABLES.TABLE_COLLATION = CCSA.COLLATION_NAME WHERE TABLES.TABLE_SCHEMA = ?";

	private Database captureDatabase(String dbName, String dbEncoding) throws SQLException, SchemaSyncError {
		PreparedStatement p = connection.prepareStatement(tblSQL);

		p.setString(1, dbName);
		ResultSet rs = p.executeQuery();

		Database db = new Database(dbName, dbEncoding);

		while ( rs.next() ) {
			Table t = db.buildTable(rs.getString("TABLE_NAME"), rs.getString("CHARACTER_SET_NAME"));
			captureTable(t);
		}

		return db;
	}


	private void captureTable(Table t) throws SQLException, SchemaSyncError {
		int i = 0;
		infoSchemaStmt.setString(1, t.getDatabase().getName());
		infoSchemaStmt.setString(2, t.getName());
		ResultSet r = infoSchemaStmt.executeQuery();

		while(r.next()) {
			String[] enumValues = null;
			String colName    = r.getString("COLUMN_NAME");
			String colType    = r.getString("DATA_TYPE");
			String colEnc     = r.getString("CHARACTER_SET_NAME");
			int colPos        = r.getInt("ORDINAL_POSITION") - 1;
			boolean colSigned = !r.getString("COLUMN_TYPE").matches(" unsigned$");

			if ( r.getString("COLUMN_KEY").equals("PRI") )
				t.pkIndex = i;

			if ( colType.equals("enum") || colType.equals("set")) {
				String expandedType = r.getString("COLUMN_TYPE");

				enumValues = extractEnumValues(expandedType);
			}

			t.addColumn(ColumnDef.build(t.getName(), colName, colEnc, colType, colPos, colSigned, enumValues));
			i++;
		}
		captureTablePK(t);
	}

	private static final String pkSQL =
			"SELECT column_name from information_schema.key_column_usage  "
	      + "WHERE constraint_name = 'PRIMARY' and table_schema = ? and table_name = ? order by ordinal_position";

	private void captureTablePK(Table t) throws SQLException, SchemaSyncError {
		PreparedStatement p = connection.prepareStatement(pkSQL);
		p.setString(1, t.getDatabase().getName());
		p.setString(2, t.getName());

		ResultSet rs = p.executeQuery();

		ArrayList<String> l = new ArrayList<>();
		while ( rs.next() ) {
			l.add(rs.getString("column_name"));
		}
		t.setPKList(l);
	}

	private static String[] extractEnumValues(String expandedType) {
		String[] enumValues;
		Matcher matcher = Pattern.compile("(enum|set)\\((.*)\\)").matcher(expandedType);
		matcher.matches(); // why do you tease me so.

		enumValues = StringUtils.split(matcher.group(2), ",");
		for(int j=0 ; j < enumValues.length; j++) {
			enumValues[j] = enumValues[j].substring(1, enumValues[j].length() - 1);
		}
		return enumValues;
	}
}
