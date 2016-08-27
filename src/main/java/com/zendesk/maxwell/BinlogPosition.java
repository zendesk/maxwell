package com.zendesk.maxwell;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public class BinlogPosition implements Serializable {
	private final long offset;
	private final String file;
	private final Long heartbeat;

	public BinlogPosition(long l, String file, Long heartbeat) {
		this.offset = l;
		this.file = file;
		this.heartbeat = heartbeat;
	}

	public BinlogPosition(long l, String file) {
		this(l, file, null);
	}

	public static BinlogPosition capture(Connection c) throws SQLException {
		ResultSet rs;
		rs = c.createStatement().executeQuery("SHOW MASTER STATUS");
		rs.next();
		return new BinlogPosition(rs.getInt("Position"), rs.getString("File"));
	}

	public static BinlogPosition at(long offset, String file) {
		return new BinlogPosition(offset, file);
	}

	public long getOffset() {
		return offset;
	}

	public String getFile() {
		return file;
	}

	public Long getHeartbeat() {
		return heartbeat;
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

	@Override
	public boolean equals(Object other) {
		if ( !(other instanceof BinlogPosition) )
			return false;
		BinlogPosition otherPosition = (BinlogPosition) other;

		return this.file.equals(otherPosition.file) && this.offset == otherPosition.offset;
	}
}
