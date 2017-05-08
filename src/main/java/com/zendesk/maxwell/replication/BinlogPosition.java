package com.zendesk.maxwell.replication;

import com.github.shyiko.mysql.binlog.GtidSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;

public class BinlogPosition implements Serializable {
	static final Logger LOGGER = LoggerFactory.getLogger(BinlogPosition.class);

	private static final String FILE_COLUMN = "File";
	private static final String POSITION_COLUMN = "Position";
	private static final String GTID_COLUMN = "Executed_Gtid_Set";

	private final String gtidSetStr;
	private final String gtid;
	private final long offset;
	private final String file;

	// LastHeartbeat is the most recent heartbeat seen prior to this position.
	// For a HeartbeatRow, it is the exact (new) heartbeat value for this position.
	// It must be set for this position to be stored or used as a restore target position.
	private final Long lastHeartbeat;

	public BinlogPosition(String gtidSetStr, String gtid, long l, String file, Long lastHeartbeat) {
		this.gtidSetStr = gtidSetStr;
		this.gtid = gtid;
		this.offset = l;
		this.file = file;
		this.lastHeartbeat = lastHeartbeat;
	}

	public BinlogPosition(long l, String file, Long lastHeartbeat) {
		this(null, null, l, file, lastHeartbeat);
	}

	public BinlogPosition(long l, String file) {
		this(null, null, l, file, null);
	}

	public static BinlogPosition capture(Connection c, boolean gtidMode) throws SQLException {
		ResultSet rs;
		rs = c.createStatement().executeQuery("SHOW MASTER STATUS");
		rs.next();
		long l = rs.getInt(POSITION_COLUMN);
		String file = rs.getString(FILE_COLUMN);
		String gtidSetStr = null;
		if (gtidMode) {
			gtidSetStr = rs.getString(GTID_COLUMN);
		}
		return new BinlogPosition(gtidSetStr, null, l, file, 0L);
	}

	public static BinlogPosition at(BinlogPosition position, Long lastHeartbeat) {
		return new BinlogPosition(position.gtidSetStr, position.gtid, position.offset, position.file, lastHeartbeat);
	}

	public static BinlogPosition at(String gtidSetStr, long offset, String file, Long lastHeartbeat) {
		return new BinlogPosition(gtidSetStr, null, offset, file, lastHeartbeat);
	}

	public static BinlogPosition at(long offset, String file, Long lastHeartbeat) {
		return new BinlogPosition(null, null, offset, file, lastHeartbeat);
	}

	public static BinlogPosition at(long offset, String file) {
		return new BinlogPosition(null, null, offset, file, null);
	}

	public long getOffset() {
		return offset;
	}

	public String getFile() {
		return file;
	}

	public Long getLastHeartbeat() {
		return lastHeartbeat;
	}

	public void requireLastHeartbeat() {
		if (lastHeartbeat == null) {
			throw new AssertionError("BinlogPosition.lastHeartbeat must be set");
		}
	}

	public String getGtid() {
		return gtid;
	}

	public String getGtidSetStr() {
		return gtidSetStr;
	}

	public GtidSet getGtidSet() {
		return new GtidSet(gtidSetStr);
	}

	@Override
	public String toString() {
		return "BinlogPosition["
			+ (gtidSetStr == null ? file + ":" + offset : gtidSetStr)
			+ ", lastHeartbeat=" + String.valueOf(lastHeartbeat)
			+ "]";
	}

	public boolean newerThan(BinlogPosition other) {
		if ( other == null )
			return true;

		if (gtidSetStr != null) {
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

		return this.file.equals(otherPosition.file)
			&& this.offset == otherPosition.offset
			&& Objects.equals(this.lastHeartbeat, otherPosition.lastHeartbeat)
			&& (gtidSetStr == null
					? otherPosition.gtidSetStr == null
					: gtidSetStr.equals(otherPosition.gtidSetStr)
				);
	}
}
