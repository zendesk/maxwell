package com.zendesk.maxwell.replication;

public class ReplicatorNotReadyException extends Exception {
	public ReplicatorNotReadyException(String message) {
		super(message);
	}
}
