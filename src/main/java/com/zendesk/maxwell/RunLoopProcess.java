package com.zendesk.maxwell;

import java.util.concurrent.TimeoutException;

abstract public class RunLoopProcess {
	private enum RunState { STOPPED, RUNNING, REQUEST_STOP };
	private volatile RunState runState;

	public RunLoopProcess() {
		this.runState = RunState.STOPPED;
	}

	protected void requestStop() {
		this.runState = RunState.REQUEST_STOP;
	}

	protected boolean isStopRequested() {
		return this.runState == RunState.REQUEST_STOP;
	}

	public boolean runLoop() throws Exception {
		if ( this.runState != RunState.STOPPED ) {
			MaxwellState.getInstance().setRunState(runState.toString());
			return false;
		}

		this.beforeStart();

		this.runState = RunState.RUNNING;
		MaxwellState.getInstance().setRunState(runState.toString());

		try {
			while (this.runState == RunState.RUNNING) {
				work();
			}
			this.beforeStop();
		} finally {
			this.runState = RunState.STOPPED;
			MaxwellState.getInstance().setRunState(runState.toString());
		}

		return true;
	}

	protected abstract void work() throws Exception;
	protected void beforeStart() throws Exception { }
	protected void beforeStop() throws Exception { }

	public void stopLoop() throws TimeoutException {
		stopLoop(5000);
	}

	public void stopLoop(long timeoutMS) throws TimeoutException {
		if ( this.runState == RunState.STOPPED )
			return;

		this.requestStop();

		for (long left = timeoutMS; left > 0 && this.runState == RunState.REQUEST_STOP; left -= 100)
			try { Thread.sleep(100); } catch (InterruptedException e) { }

		if( this.runState != RunState.STOPPED )
			throw new TimeoutException("Timed out trying to stop processed after " + timeoutMS + "ms.");
	}
}
