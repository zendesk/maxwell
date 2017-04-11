package com.zendesk.maxwell.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.concurrent.TimeoutException;

public class TaskManager {
	private static final Logger LOGGER = LoggerFactory.getLogger(TaskManager.class);

	private final ArrayList<StoppableTask> tasks;
	private volatile RunState state;

	public TaskManager() {
		this.tasks = new ArrayList<>();
		this.state = RunState.RUNNING;
	}

	// Can be invoked multiple times, will only return `true`
	// for the first invocation.
	public synchronized boolean stop(Exception error) {
		if (this.state != RunState.RUNNING) {
			LOGGER.debug("stop() called multiple times");
			return false;
		}
		this.state = RunState.REQUEST_STOP;

		LOGGER.info("stopping " + tasks.size() + " tasks");

		if (error != null) {
			LOGGER.error("cause: ", error);
		}

		// tell everything to stop
		for (StoppableTask task: this.tasks) {
			task.requestStop();
		}

		// then wait for everything to stop
		Long timeout = 500L;
		for (StoppableTask task: this.tasks) {
			try {
				task.awaitStop(timeout);
			} catch (TimeoutException e) {
				LOGGER.error(e.getMessage());
			}
		}

		this.state = RunState.STOPPED;
		LOGGER.info("stopped all tasks");
		return true;
	}

	public synchronized void add(StoppableTask task) {
		synchronized (tasks) {
			tasks.add(task);
		}
	}
}
