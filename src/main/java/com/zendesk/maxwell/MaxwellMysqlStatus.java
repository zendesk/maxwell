package com.zendesk.maxwell;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public class MaxwellMysqlStatus {
	static final Logger LOGGER = LoggerFactory.getLogger(MaxwellMysqlStatus.class);
	private Connection connection;

	public MaxwellMysqlStatus(Connection c) {
		this.connection = c;
	}

	private String sqlStatement(String variableName) {
		return "SHOW VARIABLES LIKE '" + variableName + "'";
	}

	private String getVariableState(String variableName, boolean throwOnMissing) throws SQLException, MaxwellCompatibilityError {
		ResultSet rs;

		rs = connection.createStatement().executeQuery(sqlStatement(variableName));
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

	public static void ensureReplicationMysqlState(Connection c) throws SQLException, MaxwellCompatibilityError {
		MaxwellMysqlStatus m = new MaxwellMysqlStatus(c);

		m.ensureServerIDIsSet();
		m.ensureVariableState("log_bin", "ON");
		m.ensureVariableState("binlog_format", "ROW");
		m.ensureRowImageFormat();
	}

	public static void ensureMaxwellMysqlState(Connection c) throws SQLException, MaxwellCompatibilityError {
		MaxwellMysqlStatus m = new MaxwellMysqlStatus(c);

		m.ensureVariableState("read_only", "OFF");
	}

	public static void ensureGtidMysqlState(Connection c) throws SQLException, MaxwellCompatibilityError {
		MaxwellMysqlStatus m = new MaxwellMysqlStatus(c);

		m.ensureVariableState("gtid_mode", "ON");
		m.ensureVariableState("log_slave_updates", "ON");
		m.ensureVariableState("enforce_gtid_consistency", "ON");
	}

	public static CaseSensitivity captureCaseSensitivity(Connection c) throws SQLException {
		ResultSet rs = c.createStatement().executeQuery("select @@lower_case_table_names");
		if ( !rs.next() )
			throw new RuntimeException("Could not retrieve @@lower_case_table_names!");

		int value = rs.getInt(1);
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
