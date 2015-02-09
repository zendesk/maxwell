package com.zendesk.exodus;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public class BinlogPosition {
	private final int offset;
	private final String file;

	public BinlogPosition(int offset, String file) {
		this.offset = offset;
		this.file = file;
	}

	public static BinlogPosition capture(Connection c) {
		ResultSet rs;
		try {
			rs = c.createStatement().executeQuery("SHOW MASTER STATUS");
			rs.next();
            return new BinlogPosition(rs.getInt("Position"), rs.getString("File"));
		} catch (SQLException e) {
			// TODO be smart about permission denied here
			return null;
		}
	}

	public int getOffset() {
		return offset;
	}

	public String getFile() {
		return file;
	}
}
