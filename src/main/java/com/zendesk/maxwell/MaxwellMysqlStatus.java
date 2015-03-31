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

	private String getVariableState(String variableName) throws SQLException, MaxwellCompatibilityError {
		ResultSet rs;

		rs = connection.createStatement().executeQuery(sqlStatement(variableName));
		String status;
		if(!rs.next()) {
			throw new MaxwellCompatibilityError("Could not check state for Mysql variable: "+variableName);
		}

		status = rs.getString("Value");
		return status;
	}

	private void ensureNotReadOnly() throws SQLException, MaxwellCompatibilityError {
		if (!getVariableState("read_only").equals("OFF")) {
			throw new MaxwellCompatibilityError("read_only must be OFF.");
		}
	}

	private void ensureReplicationOn() throws SQLException, MaxwellCompatibilityError {
		if (!getVariableState("log_bin").equals("ON")) {
			throw new MaxwellCompatibilityError("log_bin status must be ON.");
		}
	}

	private void ensureReplicationFormatROW() throws SQLException, MaxwellCompatibilityError {
		if (!getVariableState("binlog_format").equals("ROW")) {
			throw new MaxwellCompatibilityError("binlog_format must be ROW.");
		}
	}

	public static void ensureMysqlState(Connection c) throws SQLException, MaxwellCompatibilityError {
		MaxwellMysqlStatus m = new MaxwellMysqlStatus(c);
		m.ensureNotReadOnly();
		m.ensureReplicationOn();
		m.ensureReplicationFormatROW();
	}
}
