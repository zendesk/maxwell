package com.zendesk.maxwell;

import java.util.concurrent.TimeoutException;

/**
 * Created by ben on 10/1/15.
 */
public class RunLoopProcess {
	private enum RunState { STOPPED, RUNNING, REQUEST_STOP };
	private volatile RunState runState;

	public RunLoopProcess() {
		this.runState = RunState.STOPPED;
	}

	public boolean start() throws Exception {
		if ( this.runState != RunState.STOPPED )
			return false;

		try {
			this.beforeStart();

			this.runState = RunState.RUNNING;

			while (this.runState == RunState.RUNNING) {
				work();
			}

			this.beforeStop();
		} finally {
			this.runState = RunState.STOPPED;
		}
	}

	private abstract void work() throws Exception;
	public void beforeStart() throws Exception { }
	public void beforeStop() throws Exception { }


	public void stop() throws TimeoutException {
		stop(5000);
	}

	public void stop(long timeoutMS) throws TimeoutException {
		// note: we use stderr in this function as it's LOGGER.err() oftentimes
		// won't flush in time, and we lose the messages.
		long left = 0;


		for (left = timeoutMS; left > 0 && this.runState == RunState.REQUEST_STOP; left -= 100)
			try { Thread.sleep(100); } catch (InterruptedException e) { }

		if( this.runState != RunState.STOPPED )
			throw new TimeoutException("Timed out trying to stop processed after " + timeoutMS + "ms.");
	}
}
