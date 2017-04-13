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

	private final String gtidSetStr;
	private final String gtid;
	private final long offset;
	private final String file;
	private final Long heartbeat;

	public BinlogPosition(String gtidSetStr, String gtid, long l, String file, Long heartbeat) {
		this.gtidSetStr = gtidSetStr;
		this.gtid = gtid;
		this.offset = l;
		this.file = file;
		this.heartbeat = heartbeat;
	}

	public BinlogPosition(long l, String file, Long heartbeat) {
		this(null, null, l, file, heartbeat);
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
		return new BinlogPosition(gtidSetStr, null, l, file, null);
	}

	public static BinlogPosition at(String gtidSetStr, long offset, String file) {
		return new BinlogPosition(gtidSetStr, null, offset, file, null);
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

	public Long getHeartbeat() {
		return heartbeat;
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
		return "BinlogPosition[" +
			(gtidSetStr == null ? file + ":" + offset : gtidSetStr) + "]";
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
			&& (gtidSetStr == null
					? otherPosition.gtidSetStr == null
					: gtidSetStr.equals(otherPosition.gtidSetStr)
				);
	}
}
