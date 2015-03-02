package com.zendesk.maxwell;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public class BinlogPosition {
	private final long offset;
	private final String file;

	public BinlogPosition(long l, String file) {
		this.offset = l;
		this.file = file;
	}

	public static BinlogPosition capture(Connection c) throws SQLException {
		ResultSet rs;
		rs = c.createStatement().executeQuery("SHOW MASTER STATUS");
		rs.next();
		return new BinlogPosition(rs.getInt("Position"), rs.getString("File"));
	}

	public long getOffset() {
		return offset;
	}

	public String getFile() {
		return file;
	}

	@Override
	public String toString() {
		return "BinlogPosition[" + file + ":" + offset + "]";
	}
}
