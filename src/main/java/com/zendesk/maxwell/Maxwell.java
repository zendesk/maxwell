package com.zendesk.maxwell;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import com.zendesk.maxwell.schema.SchemaCapturer;

public class Maxwell {

	public static void main(String[] args) throws SQLException, Exception {
		Connection c = DriverManager.getConnection("jdbc:mysql://127.0.0.1:" + 3306 + "/mysql", "root", "");
		MaxwellParser p = new MaxwellParser(new SchemaCapturer(c).capture());
		p.run();
	}
}
