package com.zendesk.maxwell.util;

import java.util.concurrent.TimeoutException;

abstract public class RunLoopProcess implements StoppableTask {
	protected volatile StoppableTaskState taskState;
	private Thread thread;

	public RunLoopProcess() {
		this.taskState = new StoppableTaskState(this.getClass().getName());
	}

	public void requestStop() {
		this.taskState.requestStop();
	}

	public void awaitStop(Long timeout) throws TimeoutException {
		this.taskState.awaitStop(thread, timeout);
	}

	public void runLoop() throws Exception {
		this.thread = Thread.currentThread();
		this.beforeStart();

		try {
			while (this.taskState.isRunning()) {
				work();
			}
		} finally {
			this.beforeStop();
			this.taskState.stopped();
		}
	}

	protected abstract void work() throws Exception;
	protected void beforeStart() throws Exception { }
	protected void beforeStop() throws Exception { }
}
