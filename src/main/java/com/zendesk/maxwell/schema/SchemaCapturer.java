package com.zendesk.maxwell.schema;

import com.zendesk.maxwell.CaseSensitivity;
import com.zendesk.maxwell.schema.columndef.ColumnDef;
import org.apache.commons.lang3.StringUtils;
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

    public SchemaCapturer(Connection c, CaseSensitivity sensitivity) throws SQLException {
        this.includeDatabases = new HashSet<>();
        this.connection = c;
        this.sensitivity = sensitivity;
    }

    public SchemaCapturer(Connection c, CaseSensitivity sensitivity, String dbName) throws SQLException {
        this(c, sensitivity);
        this.includeDatabases.add(dbName);
    }

    public Schema capture() throws SQLException {
        LOGGER.debug("Capturing schema");
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

            LOGGER.debug("Registering Database " + dbName);
            Database db = new Database(dbName, charset);
            databases.add(db);
        }
        rs.close();

        LOGGER.debug("Augmenting databases with tables");
        captureDatabases(databases);
        LOGGER.debug("Databases Augmented!");

        return new Schema(databases, captureDefaultCharset(), this.sensitivity);
    }

    private String captureDefaultCharset() throws SQLException {
        LOGGER.debug("Capturing Default Charset");
        ResultSet rs = connection.createStatement().executeQuery("select @@character_set_server");
        rs.next();
        return rs.getString("@@character_set_server");
    }


    private void captureDatabases(ArrayList<Database> databases) throws SQLException {
        HashMap<String, Database> databaseNames = new HashMap<String, Database>();
        for (Database database : databases) {
            databaseNames.put(database.getName(), database);
        }

        String tblSql =
                "SELECT TABLES.TABLE_SCHEMA, TABLES.TABLE_NAME, CCSA.CHARACTER_SET_NAME "
                        + "FROM INFORMATION_SCHEMA.TABLES "
                        + "JOIN information_schema.COLLATION_CHARACTER_SET_APPLICABILITY AS CCSA"
                        + " ON TABLES.TABLE_COLLATION = CCSA.COLLATION_NAME WHERE " + databaseConditional("TABLES.TABLE_SCHEMA");

        PreparedStatement p = connection.prepareStatement(tblSql);
        ResultSet rs = p.executeQuery();

        HashMap<String, Table> tables = new HashMap<>();
        while (rs.next()) {
            String databaseName = rs.getString("TABLE_SCHEMA");
            String tableName = rs.getString("TABLE_NAME");
            String characterSetName = rs.getString("CHARACTER_SET_NAME");
            String tableKey = tableKey(databaseName, tableName);
            LOGGER.debug("Capturing table " + tableKey);

            Database db = databaseNames.get(databaseName);
            Table t = db.buildTable(tableName, characterSetName);
            tables.put(tableKey, t);
        }
        rs.close();

        LOGGER.debug("Augmenting tables");
        captureTables(tables);
    }


    private boolean isMySQLAtLeast56() throws SQLException {
        java.sql.DatabaseMetaData meta = connection.getMetaData();
        int major = meta.getDatabaseMajorVersion();
        int minor = meta.getDatabaseMinorVersion();
        return ((major == 5 && minor >= 6) || major > 5);
    }


    private void captureTables(HashMap<String, Table> tables) throws SQLException {

        String sql = "SELECT " +
                "TABLE_SCHEMA, " +
                "TABLE_NAME," +
                "COLUMN_NAME, " +
                "DATA_TYPE, " +
                "CHARACTER_SET_NAME, " +
                "ORDINAL_POSITION, " +
                "COLUMN_TYPE, " +
                "DATETIME_PRECISION, " +
                "COLUMN_KEY, " +
                "COLUMN_TYPE " +
                "FROM `information_schema`.`COLUMNS` WHERE " + databaseConditional("TABLE_SCHEMA");

        PreparedStatement statement = connection.prepareStatement(sql);
        ResultSet r = statement.executeQuery();

        boolean hasDatetimePrecision = isMySQLAtLeast56();

        HashMap<String, Integer> pkIndexCounters = new HashMap<>();
        for (String tableKey : tables.keySet()) {
            pkIndexCounters.put(tableKey, 0);
        }

        while (r.next()) {
            String[] enumValues = null;
            String dbName = r.getString("TABLE_SCHEMA");
            String tableName = r.getString("TABLE_NAME");
            String tableKey = tableKey(dbName, tableName);

            if (tables.containsKey(tableKey)) {
                LOGGER.debug("Augmenting table " + tableKey);
                Table t = tables.get(tableKey);
                String colName = r.getString("COLUMN_NAME");
                String colType = r.getString("DATA_TYPE");
                String colEnc = r.getString("CHARACTER_SET_NAME");
                int colPos = r.getInt("ORDINAL_POSITION") - 1;
                boolean colSigned = !r.getString("COLUMN_TYPE").matches(".* unsigned$");
                Long columnLength = null;

                if (hasDatetimePrecision)
                    columnLength = r.getLong("DATETIME_PRECISION");

                if (r.getString("COLUMN_KEY").equals("PRI"))
                    t.pkIndex = pkIndexCounters.get(tableKey);

                if (colType.equals("enum") || colType.equals("set")) {
                    String expandedType = r.getString("COLUMN_TYPE");

                    enumValues = extractEnumValues(expandedType);
                }

                t.addColumn(ColumnDef.build(colName, colEnc, colType, colPos, colSigned, enumValues, columnLength));

                pkIndexCounters.put(tableKey, pkIndexCounters.get(tableKey) + 1);
            }
        }
        r.close();

        LOGGER.debug("Augmenting table primary keys");
        captureTablesPK(tables);
    }

    private void captureTablesPK(HashMap<String, Table> tables) throws SQLException {

        String pkSQL = "SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION FROM information_schema.KEY_COLUMN_USAGE "
                + "WHERE CONSTRAINT_NAME = 'PRIMARY' AND " + databaseConditional("TABLE_SCHEMA");

        PreparedStatement p = connection.prepareStatement(pkSQL);
        ResultSet rs = p.executeQuery();

        HashMap<String, ArrayList<String>> l = new HashMap<>();

        for (String tableKey : tables.keySet()) {
            l.put(tableKey, new ArrayList<String>());
        }

        while (rs.next()) {
            int ordinalPosition = rs.getInt("ORDINAL_POSITION");
            String dbName = rs.getString("TABLE_SCHEMA");
            String tableName = rs.getString("TABLE_NAME");
            String columnName = rs.getString("COLUMN_NAME");
            String tableKey = tableKey(dbName, tableName);

            l.get(tableKey).add(ordinalPosition - 1, columnName);
        }
        rs.close();

        for (Map.Entry<String, Table> entry : tables.entrySet()) {
            String key = entry.getKey();
            Table table = entry.getValue();

            table.setPKList(l.get(key));
        }
    }

    private static String[] extractEnumValues(String expandedType) {
        String[] enumValues;
        Matcher matcher = Pattern.compile("(enum|set)\\((.*)\\)").matcher(expandedType);
        matcher.matches(); // why do you tease me so.

        enumValues = StringUtils.split(matcher.group(2), ",");
        for (int j = 0; j < enumValues.length; j++) {
            enumValues[j] = enumValues[j].substring(1, enumValues[j].length() - 1);
        }
        return enumValues;
    }


    private String databaseConditional(String schemaAlias) {
        String where = schemaAlias + " NOT IN ('" + StringUtils.join(IGNORED_DATABASES, "','") + "') ";

        if (includeDatabases.size() > 0) {
            where = " AND " + schemaAlias + " IN ('" + StringUtils.join(includeDatabases, "','") + "') ";
        }

        return where;
    }


    private String tableKey(String databaseName, String tableName) {
        return databaseName + "::" + tableName;
    }
}
