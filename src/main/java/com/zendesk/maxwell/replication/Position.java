package com.zendesk.maxwell.replication;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;

import com.zendesk.maxwell.replication.vitess.Vgtid;

public class Position implements Serializable {
	// LastHeartbeat is the most recent heartbeat seen prior to this position.
	// For a HeartbeatRow, it is the exact (new) heartbeat value for this position.
	private final long lastHeartbeatRead;
	private final BinlogPosition binlogPosition;
	private final Vgtid vgtid;

	public Position(BinlogPosition binlogPosition, long lastHeartbeatRead) {
		this.binlogPosition = binlogPosition;
		this.lastHeartbeatRead = lastHeartbeatRead;
		this.vgtid = null;
	}

	// Vitess-related constructor
	// FIXME: We may want to introduce a separate position class for Vitess
	public Position(Vgtid vgtid) {
		this.binlogPosition = null;
		this.lastHeartbeatRead = 0L;
		this.vgtid = vgtid;
	}

	public static Position valueOf(BinlogPosition binlogPosition, Long lastHeartbeatRead) {
		return new Position(binlogPosition, lastHeartbeatRead);
	}

	public static Position valueOf(Vgtid vgtid) {
		return new Position(vgtid);
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

	public Vgtid getVgtid() {
		return vgtid;
	}

	public Position addGtid(String gtid, long offset, String file) {
		return new Position(binlogPosition.addGtid(gtid, offset, file), lastHeartbeatRead);
	}

	@Override
	public String toString() {
		if (vgtid == null) {
			return "Position[" + binlogPosition + ", lastHeartbeat=" + lastHeartbeatRead + "]";
		} else {
			return "Position[" + vgtid + "]";
		}
	}

	public String toCommandline() {
		String gtid = binlogPosition.getGtidSetStr();
		if ( gtid != null )
			return gtid;
		else if (vgtid != null)
			return vgtid.toString();
		else
			return binlogPosition.getFile() + ":" + binlogPosition.getOffset();
	}

	@Override
	public boolean equals(Object o) {
		if ( !(o instanceof Position) ) {
			return false;
		}
		Position other = (Position) o;

		if (vgtid != null) {
			return vgtid.equals(other.vgtid);
		} else {
			return lastHeartbeatRead == other.lastHeartbeatRead && binlogPosition.equals(other.binlogPosition);
		}
	}

	@Override
	public int hashCode() {
		if (vgtid != null) {
			return vgtid.hashCode();
		} else {
			return binlogPosition.hashCode();
		}
	}

	public boolean newerThan(Position other) {
		if ( other == null )
			return true;

		if (vgtid != null) {
			// FIXME: Implement actual newerThan comparison for Vgtid values, for now just
			// check if it is different to avoid persisting the same position over and over
			return !vgtid.equals(other.vgtid);
		} else {
			return this.getBinlogPosition().newerThan(other.getBinlogPosition());
		}
	}
}
