package com.zendesk.maxwell;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public class MaxwellMysqlStatus {
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

	private void ensureNotReadOnly() throws SQLException, MaxwellCompatibilityError {
		if (!getVariableState("read_only", true).equals("OFF")) {
			throw new MaxwellCompatibilityError("read_only must be OFF.");
		}
	}

	private void ensureReplicationOn() throws SQLException, MaxwellCompatibilityError {
		if (!getVariableState("log_bin", true).equals("ON")) {
			throw new MaxwellCompatibilityError("log_bin status must be ON.");
		}
	}

	private void ensureReplicationFormatROW() throws SQLException, MaxwellCompatibilityError {
		if (!getVariableState("binlog_format", true).equals("ROW")) {
			throw new MaxwellCompatibilityError("binlog_format must be ROW.");
		}
	}

	private void ensureRowImageFormat() throws SQLException, MaxwellCompatibilityError {
		String rowImageFormat = getVariableState("binlog_row_image", false);
		if ( rowImageFormat == null ) // only present in mysql 5.6+
			return;

		if ( !rowImageFormat.equals("FULL"))
			throw new MaxwellCompatibilityError("binlog_row_image must be FULL.");
	}

	public static void ensureMysqlState(Connection c) throws SQLException, MaxwellCompatibilityError {
		MaxwellMysqlStatus m = new MaxwellMysqlStatus(c);
		m.ensureNotReadOnly();
		m.ensureReplicationOn();
		m.ensureReplicationFormatROW();
		m.ensureRowImageFormat();
	}
}
