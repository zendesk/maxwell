package com.zendesk.maxwell.replication;

import com.zendesk.maxwell.replication.vitess.Vgtid;

public class VitessPosition extends Position {
	private final Vgtid vgtid;

	public VitessPosition(Vgtid vgtid) {
		super(null, 0L);
		this.vgtid = vgtid;
	}

	public static VitessPosition valueOf(Vgtid vgtid) {
		return new VitessPosition(vgtid);
	}

	public Vgtid getVgtid() {
		return vgtid;
	}

	@Override
	public String toString() {
		return "Position[" + vgtid + "]";
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof VitessPosition) {
			VitessPosition other = (VitessPosition) o;
			return vgtid.equals(other.vgtid);
		}
		return false;
	}

	@Override
	public int hashCode() {
		return vgtid.hashCode();
	}

	public boolean newerThan(Position other) {
		if (other instanceof VitessPosition) {
			// FIXME: Implement actual newerThan comparison for Vgtid values
			// For now just check if it is different to avoid persisting the same position
			// multiple times
			VitessPosition vOther = (VitessPosition) other;
			return !vgtid.equals(vOther.vgtid);
		}
		return true;
	}
}
