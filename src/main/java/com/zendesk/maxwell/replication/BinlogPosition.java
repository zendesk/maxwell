package com.zendesk.maxwell.replication;

import com.github.shyiko.mysql.binlog.GtidSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public class BinlogPosition implements Serializable {
	static final Logger LOGGER = LoggerFactory.getLogger(BinlogPosition.class);

	private static final String FILE_COLUMN = "File";
	private static final String POSITION_COLUMN = "Position";
	private static final String GTID_COLUMN = "Executed_Gtid_Set";

	private final String gtidStr;
	private final long offset;
	private final String file;
	private final Long heartbeat;

	public BinlogPosition(String gtidStr, long l, String file, Long heartbeat) {
		this.gtidStr = gtidStr;
		this.offset = l;
		this.file = file;
		this.heartbeat = heartbeat;
	}

	public BinlogPosition(long l, String file, Long heartbeat) {
		this(null, l, file, heartbeat);
	}

	public BinlogPosition(long l, String file) {
		this(null, l, file, null);
	}

	public static BinlogPosition capture(Connection c, boolean gtidMode) throws SQLException {
		ResultSet rs;
		rs = c.createStatement().executeQuery("SHOW MASTER STATUS");
		rs.next();
		long l = rs.getInt(POSITION_COLUMN);
		String file = rs.getString(FILE_COLUMN);
		String gtidStr = null;
		if (gtidMode) {
			gtidStr = rs.getString(GTID_COLUMN);
		}
		return new BinlogPosition(gtidStr, l, file, null);
	}

	public static BinlogPosition at(String gtidStr, long offset, String file) {
		return new BinlogPosition(gtidStr, offset, file, null);
	}

	public static BinlogPosition at(long offset, String file) {
		return new BinlogPosition(null, offset, file, null);
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

	public String getGtidStr() {
		return gtidStr;
	}

	public GtidSet getGtidSet() {
		return new GtidSet(gtidStr);
	}

	@Override
	public String toString() {
		return "BinlogPosition[" +
			(gtidStr == null ? file + ":" + offset : gtidStr) + "]";
	}

	public boolean newerThan(BinlogPosition other) {
		if ( other == null )
			return true;

		if (gtidStr != null) {
			return !getGtidSet().isContainedWithin(other.getGtidSet());
		}

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

		return this.file.equals(otherPosition.file) && this.offset == otherPosition.offset
			&& (gtidStr == null) ? otherPosition.gtidStr == null
				: gtidStr.equals(otherPosition.gtidStr);
	}
}
