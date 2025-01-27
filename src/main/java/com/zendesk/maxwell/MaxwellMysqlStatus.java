package com.zendesk.maxwell;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;
import java.util.Properties;

/**
 * Class with some utility functions for querying mysql server state
 */
public class MaxwellMysqlStatus {
	static final Logger LOGGER = LoggerFactory.getLogger(MaxwellMysqlStatus.class);
	private Connection connection;

	public MaxwellMysqlStatus(Connection c) {
		this.connection = c;
	}

	private String sqlStatement(String variableName) {
		return "SHOW VARIABLES LIKE '" + variableName + "'";
	}
	public boolean isMaria() {
		try {
			DatabaseMetaData md = connection.getMetaData();
			return md.getDatabaseProductVersion().toLowerCase().contains("mariadb");
		} catch ( SQLException e ) {
			return false;
		}
	}

       /**
       * Generates the appropriate SQL command to retrieve binary log status based on
       * the database type and version.
       *
       * This method checks the database product name and version to determine
       * the most suitable SQL command for retrieving binary log status information.
       * It supports MySQL and MariaDB, with compatibility for recent version
       * requirements:
       * <ul>
       * <li>MySQL 8.2 and above: uses "SHOW BINARY LOG STATUS"</li>
       * <li>MariaDB 10.5 and above: uses "SHOW BINLOG STATUS"</li>
       * <li>All other versions default to "SHOW MASTER STATUS"</li>
       * </ul>
       * If an error occurs during metadata retrieval, the method defaults to "SHOW
       * MASTER STATUS".
       *
       * @return a SQL command string to check binary log status
       */
	public String getShowBinlogSQL() {
		try {
			DatabaseMetaData md = connection.getMetaData();

			String productName = md.getDatabaseProductVersion();

			int majorVersion = md.getDatabaseMajorVersion();
			int minorVersion = md.getDatabaseMinorVersion();

			boolean isMariaDB = productName.toLowerCase().contains("mariadb");
			boolean isMySQL = !isMariaDB;

			if (isMySQL && (majorVersion > 8 || (majorVersion == 8 && minorVersion >= 2))) {
				return "SHOW BINARY LOG STATUS";
			} else if (isMariaDB && (majorVersion > 10 || (majorVersion == 10 && minorVersion >= 5))) {
				return "SHOW BINLOG STATUS";
			}
		} catch ( SQLException e ) {
			return "SHOW MASTER STATUS";
		}

		return "SHOW MASTER STATUS";
	}




	public String getVariableState(String variableName, boolean throwOnMissing) throws SQLException, MaxwellCompatibilityError {
		try ( Statement stmt = connection.createStatement();
		      ResultSet rs = stmt.executeQuery(sqlStatement(variableName)) ) {
			String status;
			if(!rs.next()) {
				if ( throwOnMissing ) {
					throw new MaxwellCompatibilityError("Could not check state for Mysql variable: " + variableName);
				} else {
					return null;
				}
			}

			status = rs.getString("Value");
			return status;
		}
	}
	public String getVariableState(String variableName) throws SQLException {
		try {
			return getVariableState(variableName, false);
		} catch ( MaxwellCompatibilityError e ) {
			return null;
		}
	}

	private void ensureVariableState(String variable, String state) throws SQLException, MaxwellCompatibilityError
	{
		if (!getVariableState(variable, true).equals(state)) {
			throw new MaxwellCompatibilityError("variable " + variable + " must be set to '" + state + "'");
		}
	}


	private void ensureServerIDIsSet() throws SQLException, MaxwellCompatibilityError {
		String id = getVariableState("server_id", false);
		if ( "0".equals(id) ) {
			throw new MaxwellCompatibilityError("server_id is '0'.  Maxwell will not function without a server_id being set.");
		}
	}

	private void ensureRowImageFormat() throws SQLException, MaxwellCompatibilityError {
		String rowImageFormat = getVariableState("binlog_row_image", false);
		if ( rowImageFormat == null ) // only present in mysql 5.6+
			return;

		if ( rowImageFormat.equals("MINIMAL") ) {
			LOGGER.warn("Warning: binlog_row_image is set to MINIMAL.  This may not be what you want.");
			LOGGER.warn("See http://maxwells-daemon.io/compat for more information.");
		}
	}

	/**
	 * Verify that replication is in the expected state:
	 *
	 * <ol>
	 *     <li>Check that a serverID is set</li>
	 *     <li>check that binary logging is on</li>
	 *     <li>Check that the binlog_format is "ROW"</li>
	 *     <li>Warn if binlog_row_image is MINIMAL</li>
	 * </ol>
	 * @param c a JDBC connection
	 * @throws SQLException if the database has issues
	 * @throws MaxwellCompatibilityError if we are not in the expected state
	 */
	public static void ensureReplicationMysqlState(Connection c) throws SQLException, MaxwellCompatibilityError {
		MaxwellMysqlStatus m = new MaxwellMysqlStatus(c);

		m.ensureServerIDIsSet();
		m.ensureVariableState("log_bin", "ON");
		m.ensureVariableState("binlog_format", "ROW");
		m.ensureRowImageFormat();
	}

	/**
	 * Verify that the maxwell database is in the expected state
	 * @param c a JDBC connection
	 * @throws SQLException if we have database issues
	 * @throws MaxwellCompatibilityError if we're not in the expected state
	 */
	public static void ensureMaxwellMysqlState(Connection c) throws SQLException, MaxwellCompatibilityError {
		MaxwellMysqlStatus m = new MaxwellMysqlStatus(c);

		m.ensureVariableState("read_only", "OFF");
	}

	/**
	 * Verify that we can safely turn on maxwell GTID mode
	 * @param c a JDBC connection
	 * @throws SQLException if we have db troubles
	 * @throws MaxwellCompatibilityError if we're not in the expected state
	 */
	public static void ensureGtidMysqlState(Connection c) throws SQLException, MaxwellCompatibilityError {
		MaxwellMysqlStatus m = new MaxwellMysqlStatus(c);

		if ( m.isMaria() )
			return;

		m.ensureVariableState("gtid_mode", "ON");
		m.ensureVariableState("log_slave_updates", "ON");
		m.ensureVariableState("enforce_gtid_consistency", "ON");
	}

	public static boolean isMaria(Connection c) {
		MaxwellMysqlStatus m = new MaxwellMysqlStatus(c);
		return m.isMaria();
	}

	/**
	 * Return an enum representing the current case sensitivity of the server
	 * @param c a JDBC connection
	 * @return case sensitivity
	 * @throws SQLException if we have db troubles
	 */
	public static CaseSensitivity captureCaseSensitivity(Connection c) throws SQLException {
		final int value;
		try ( Statement stmt = c.createStatement();
			  ResultSet rs = stmt.executeQuery("select @@lower_case_table_names") ) {
			if ( !rs.next() )
				throw new RuntimeException("Could not retrieve @@lower_case_table_names!");
			value = rs.getInt(1);
		}

		switch(value) {
			case 0:
				return CaseSensitivity.CASE_SENSITIVE;
			case 1:
				return CaseSensitivity.CONVERT_TO_LOWER;
			case 2:
				return CaseSensitivity.CONVERT_ON_COMPARE;
			default:
				throw new RuntimeException("Unknown value for @@lower_case_table_names: " + value);
		}
	}
}
