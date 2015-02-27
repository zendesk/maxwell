package com.zendesk.maxwell.schema;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zendesk.maxwell.schema.columndef.ColumnDef;

public class SchemaCapturer {
	private final Connection connection;
	static final Logger LOGGER = LoggerFactory.getLogger(SchemaStore.class);

	private final String[] alwaysExclude = {"performance_schema", "information_schema", "mysql"};
	private final HashSet<String> excludeDatabases;
	private final HashSet<String> includeDatabases;

	private final PreparedStatement infoSchemaStmt;
	private final String INFORMATION_SCHEMA_SQL =
			"SELECT * FROM `information_schema`.`COLUMNS` WHERE `TABLE_SCHEMA` = ? AND `TABLE_NAME` = ?";

	public SchemaCapturer(Connection c) throws SQLException {
		this.excludeDatabases = new HashSet<String>();
		this.includeDatabases = new HashSet<String>();
		this.connection = c;
		this.infoSchemaStmt = connection.prepareStatement(INFORMATION_SCHEMA_SQL);

		for (String s : alwaysExclude) {
			this.excludeDatabases.add(s);
		}
	}

	public SchemaCapturer(Connection c, String dbName) throws SQLException {
		this(c);
		this.includeDatabases.add(dbName);
	}

	public Schema capture() throws SQLException {
		LOGGER.debug("Capturing schema");
		ArrayList<Database> databases = new ArrayList<Database>();


		ResultSet rs = connection.createStatement().executeQuery("SELECT * from INFORMATION_SCHEMA.SCHEMATA");

		while ( rs.next() ) {
			String dbName = rs.getString("SCHEMA_NAME");
			String encoding = rs.getString("DEFAULT_CHARACTER_SET_NAME");

			if ( includeDatabases.size() > 0 && !includeDatabases.contains(dbName))
				continue;

			if ( excludeDatabases.contains(dbName) )
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


	private Database captureDatabase(String dbName, String dbEncoding) throws SQLException {
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


	private void captureTable(Table t) throws SQLException {
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

				Matcher matcher = Pattern.compile("(enum|set)\\((.*)\\)").matcher(expandedType);
				matcher.matches(); // why do you tease me so.

				enumValues = StringUtils.split(matcher.group(1), ",");
				for(int j=0 ; j < enumValues.length; j++) {
					enumValues[j] = enumValues[j].substring(1, enumValues[j].length() - 1);
				}
			}

			t.getColumnList().add(ColumnDef.build(t.getName(), colName, colEnc, colType, colPos, colSigned, enumValues));
			i++;
		}
	}
}
