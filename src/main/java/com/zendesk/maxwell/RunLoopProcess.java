package com.zendesk.maxwell;

import java.util.concurrent.TimeoutException;

abstract public class RunLoopProcess {
	private enum RunState { STOPPED, RUNNING, REQUEST_STOP };
	private volatile RunState runState;
	private Thread thread;

	public RunLoopProcess() {
		this.runState = RunState.STOPPED;
	}

	protected void requestStop() {
		if ( this.runState != RunState.STOPPED )
			this.runState = RunState.REQUEST_STOP;

	}

	protected boolean isStopRequested() {
		return this.runState == RunState.REQUEST_STOP;
	}

	public boolean runLoop() throws Exception {
		if ( this.runState != RunState.STOPPED )
			return false;

		this.thread = Thread.currentThread();
		this.beforeStart();

		this.runState = RunState.RUNNING;

		try {
			while (this.runState == RunState.RUNNING)
				work();

		} finally {
			this.beforeStop();
			this.runState = RunState.STOPPED;
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

		/* gentle request */
		this.requestStop();

		/* foot tapping */
		for (long left = timeoutMS; left > 0 && this.runState == RunState.REQUEST_STOP; left -= 10)
			try { Thread.sleep(10); } catch (InterruptedException e) { }

		/* very impatient throat clear */
		thread.interrupt();

		try { Thread.sleep(50); } catch (InterruptedException e) { }

		if( this.runState != RunState.STOPPED )
			throw new TimeoutException("Timed out trying to stop processed after " + timeoutMS + "ms.");
	}
}
