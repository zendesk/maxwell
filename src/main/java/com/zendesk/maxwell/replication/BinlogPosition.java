package com.zendesk.maxwell.replication;

import com.github.shyiko.mysql.binlog.GtidSet;

import com.github.shyiko.mysql.binlog.MariadbGtidSet;
import com.zendesk.maxwell.MaxwellCompatibilityError;
import com.zendesk.maxwell.MaxwellMysqlStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class BinlogPosition implements Serializable {
	static final Logger LOGGER = LoggerFactory.getLogger(BinlogPosition.class);

	private static final String FILE_COLUMN = "File";
	private static final String POSITION_COLUMN = "Position";
	private static final String GTID_COLUMN = "Executed_Gtid_Set";

	private String gtidSetStr;
	private final String gtid;
	private final long offset;
	private final Long fileNumber;
	private final String file;

	public BinlogPosition(String gtidSetStr, String gtid, long l, String file) {
		this.gtidSetStr = gtidSetStr;
		this.gtid = gtid;
		this.offset = l;
		this.file = file;
		this.fileNumber = parseFileNumber(file);
	}

	public BinlogPosition(long l, String file) {
		this(null, null, l, file);
	}

	public static BinlogPosition capture(Connection c, boolean gtidMode) throws SQLException {
		MaxwellMysqlStatus m = new MaxwellMysqlStatus(c);

		try ( Statement stmt = c.createStatement();
		      ResultSet rs = stmt.executeQuery("SHOW MASTER STATUS") ) {
			rs.next();
			long l = rs.getInt(POSITION_COLUMN);
			String file = rs.getString(FILE_COLUMN);
			String gtidSetStr = null;
			if (gtidMode) {
				if ( m.isMaria() ) {
					gtidSetStr = m.getVariableState("gtid_binlog_state");
				} else {
					gtidSetStr = rs.getString(GTID_COLUMN);
				}
			}
			return new BinlogPosition(gtidSetStr, null, l, file);
		}
	}

	public static BinlogPosition at(BinlogPosition position) {
		return new BinlogPosition(position.gtidSetStr, position.gtid, position.offset, position.file);
	}

	public static BinlogPosition at(String gtidSetStr, long offset, String file) {
		return new BinlogPosition(gtidSetStr, null, offset, file);
	}

	public static BinlogPosition at(long offset, String file) {
		return new BinlogPosition(null, null, offset, file);
	}

	public long getOffset() {
		return offset;
	}

	public String getFile() {
		return file;
	}

	public String getGtid() {
		return gtid;
	}

	public String getGtidSetStr() {
		return gtidSetStr;
	}

	public BinlogPosition addGtid(String gtid, long offset, String file) {
		GtidSet set = this.getGtidSet();
		if ( set == null )
			return new BinlogPosition(offset, file);

		set.add(gtid);
		return new BinlogPosition(set.toSeenString(), gtid, offset, file);
	}

	public void mergeGtids(GtidSet seenSet) {
		if ( seenSet == null ) {
			this.gtidSetStr = this.gtid;
		} else {
			seenSet.add(this.getGtid());
			this.gtidSetStr = seenSet.toSeenString();
		}
	}

	public GtidSet getGtidSet() {
		if ( gtidSetStr == null )
			return null;

		if ( MariadbGtidSet.isMariaGtidSet(gtidSetStr) )
			return new MariadbGtidSet((gtidSetStr));
		else
			return new GtidSet(gtidSetStr);
	}

	@Override
	public String toString() {
		return "BinlogPosition["
			+ (gtidSetStr == null ? file + ":" + offset : gtidSetStr)
			+ "]";
	}

	public String fullPosition() {
		String pos = file + ":" + offset;
		if ( gtidSetStr != null )
			pos += "[" + gtidSetStr + "]";
		return pos;
	}

	private Long parseFileNumber(String filename) {
		String[] split = filename.split("\\.");
		if ( split.length < 2 ) {
			return null;
		} else {
			return Long.valueOf(split[split.length - 1]);
		}
	}

	public Long getFileNumber() {
		return this.fileNumber;
	}


	public boolean newerThan(BinlogPosition other) {
		if ( other == null )
			return true;

		if (gtidSetStr != null) {
			return !getGtidSet().isContainedWithin(other.getGtidSet());
		}

		long cmp;
		if ( this.fileNumber == null || other.getFileNumber() == null ) {
			cmp = this.file.compareTo(other.file);
		} else {
			cmp = this.fileNumber - other.getFileNumber();
		}

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

	@Override
	public int hashCode() {
		if (gtidSetStr != null) {
			return gtidSetStr.hashCode();
		} else {
			return Long.valueOf(offset).hashCode();
		}
	}

}
