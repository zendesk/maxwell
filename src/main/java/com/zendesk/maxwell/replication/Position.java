package com.zendesk.maxwell.replication;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;

public class Position implements Serializable {
	// LastHeartbeat is the most recent heartbeat seen prior to this position.
	// For a HeartbeatRow, it is the exact (new) heartbeat value for this position.
	private final long lastHeartbeatRead;
	private final BinlogPosition binlogPosition;

	public Position(BinlogPosition binlogPosition, long lastHeartbeatRead) {
		this.binlogPosition = binlogPosition;
		this.lastHeartbeatRead = lastHeartbeatRead;
	}

	public static Position valueOf(BinlogPosition binlogPosition, Long lastHeartbeatRead) {
		return new Position(binlogPosition, lastHeartbeatRead);
	}

	public Position withHeartbeat(long lastHeartbeatRead) {
		return new Position(getBinlogPosition(), lastHeartbeatRead);
	}

	public static Position capture(Connection c, boolean gtidMode) throws SQLException {
		return new Position(BinlogPosition.capture(c, gtidMode), 0L);
	}

	public long getLastHeartbeatRead() {
		return lastHeartbeatRead;
	}

	public BinlogPosition getBinlogPosition() {
		return binlogPosition;
	}

	public Position addGtid(String gtid, long offset, String file) {
		return new Position(binlogPosition.addGtid(gtid, offset, file), lastHeartbeatRead);
	}

	@Override
	public String toString() {
		return "Position[" + binlogPosition + ", lastHeartbeat=" + lastHeartbeatRead + "]";
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
		if ( other == null )
			return true;
		return this.getBinlogPosition().newerThan(other.getBinlogPosition());
	}
}
