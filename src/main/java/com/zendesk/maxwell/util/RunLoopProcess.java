package com.zendesk.maxwell.util;

import java.util.concurrent.TimeoutException;

abstract public class RunLoopProcess implements StoppableTask {
	protected volatile StoppableTaskState taskState;
	private Thread thread;

	public RunLoopProcess() {
	}

	public void requestStop() {
		this.taskState.requestStop();
	}

	public void awaitStop(Long timeout) throws TimeoutException {
		this.taskState.awaitStop(thread, timeout);
	}

	public boolean runLoop() throws Exception {
		if ( this.taskState != null )
			return false;

		this.taskState = new StoppableTaskState(this.getClass().getName());
		this.thread = Thread.currentThread();
		this.beforeStart();

		try {
			while (this.taskState.keepGoing()) {
				work();
			}
		} finally {
			this.beforeStop();
			this.taskState.stopped();
		}

		return true;
	}

	protected abstract void work() throws Exception;
	protected void beforeStart() throws Exception { }
	protected void beforeStop() throws Exception { }
}
