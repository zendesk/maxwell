package com.zendesk.maxwell.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeoutException;

public class StoppableTaskState {
	private static Logger LOGGER = LoggerFactory.getLogger(StoppableTaskState.class);
	private volatile RunState state;
	private final String description;

	public StoppableTaskState(String description) {
		state = RunState.RUNNING;
		this.description = description;
	}

	public boolean isRunning() {
		return state == RunState.RUNNING;
	}

	public synchronized void requestStop() {
		LOGGER.info(description + " requestStop() called (in state: " + state + ")");
		if (isRunning()) {
			this.state = RunState.REQUEST_STOP;
		}
	}

	public void stopped() {
		this.state = RunState.STOPPED;
	}

	public synchronized void awaitStop(Thread t, long timeoutMS) throws TimeoutException {
		/* foot tapping */
		for (long left = timeoutMS; left > 0 && this.state == RunState.REQUEST_STOP; left -= 10)
			try { Thread.sleep(10); } catch (InterruptedException e) { }

		/* very impatient throat clear */
		if (t != null) {
			t.interrupt();
		}

		try { Thread.sleep(100); } catch (InterruptedException e) { }

		if( this.state != RunState.STOPPED ) {
			throw new TimeoutException(
				"Timed out trying waiting for " + description + " process to stop after " + timeoutMS + "ms."
			);
		}
	}

	public RunState getState() {
		return state;
	}
}
