package com.zendesk.maxwell.replication;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;

public class Position implements Serializable {
	// LastHeartbeat is the most recent heartbeat seen prior to this position.
	// For a HeartbeatRow, it is the exact (new) heartbeat value for this position.
	private final long lastHeartbeatRead;
	private final BinlogPosition binlogPosition;
	private final long txOffset;

	public Position(BinlogPosition binlogPosition, long lastHeartbeatRead, long txOffset) {
		this.binlogPosition = binlogPosition;
		this.lastHeartbeatRead = lastHeartbeatRead;
		this.txOffset = txOffset;
	}

	public Position(BinlogPosition binlogPosition, long lastHeartbeatRead) {
		this(binlogPosition, lastHeartbeatRead, 0L);
	}

	public static Position valueOf(BinlogPosition binlogPosition, Long lastHeartbeatRead, long txOffset) {
		return new Position(binlogPosition, lastHeartbeatRead, txOffset);
	}

	public static Position capture(Connection c, boolean gtidMode) throws SQLException {
		return new Position(BinlogPosition.capture(c, gtidMode), 0L, 0L);
	}

	public Position withHeartbeat(long lastHeartbeatRead) {
		return new Position(getBinlogPosition(), lastHeartbeatRead, txOffset);
	}

	public long getLastHeartbeatRead() {
		return lastHeartbeatRead;
	}

	public BinlogPosition getBinlogPosition() {
		return binlogPosition;
	}

	public long getTXOffset() {
		return txOffset;
	}

	@Override
	public String toString() {
		String s = 	"Position[" + binlogPosition + ", lastHeartbeat=" + lastHeartbeatRead + "]";
		if ( txOffset > 0 ) {
			s += "+" + txOffset;
		}
		return s;
	}

	public String toCommandline() {
		String gtid = binlogPosition.getGtidSetStr();
		if ( gtid != null )
			return gtid;
		else
			return binlogPosition.getFile() + ":" + binlogPosition.getOffset();
	}

	@Override
	public boolean equals(Object o) {
		if ( !(o instanceof Position) ) {
			return false;
		}
		Position other = (Position) o;

		return lastHeartbeatRead == other.lastHeartbeatRead
			&& binlogPosition.equals(other.binlogPosition);
	}

	@Override
	public int hashCode() {
		return binlogPosition.hashCode();
	}

	public boolean newerThan(Position other) {
		BinlogPosition ours = this.getBinlogPosition();
		BinlogPosition theirs = other.getBinlogPosition();
		if ( other == null )
			return true;
		return ours.newerThan(theirs) || (ours.equals(theirs) && this.txOffset > other.txOffset);
	}

	public Position withTXOffset(Long txOffset) {
		return new Position(binlogPosition, lastHeartbeatRead, txOffset);
	}
}
