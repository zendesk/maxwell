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

	private void ensureVariableState(String variable, String state) throws SQLException, MaxwellCompatibilityError
	{
		if (!getVariableState(variable, true).equals(state)) {
			throw new MaxwellCompatibilityError("variable " + variable + " must be set to '" + state + "'");
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

		m.ensureVariableState("read_only", "OFF");
		m.ensureVariableState("log_bin", "ON");
		m.ensureVariableState("binlog_format", "ROW");
		m.ensureRowImageFormat();
	}
}
