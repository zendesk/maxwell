package com.zendesk.maxwell;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public class BinlogPosition implements Serializable {
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

	public boolean newerThan(BinlogPosition other) {
		if ( other == null )
			return true;

		int cmp = this.file.compareTo(other.file);
		if ( cmp > 0 ) {
			return true;
		} else if ( cmp == 0 ) {
			return this.offset > other.offset;
		} else {
			return false;
		}
	}
}
