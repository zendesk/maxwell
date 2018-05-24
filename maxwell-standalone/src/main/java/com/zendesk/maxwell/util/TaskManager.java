package com.zendesk.maxwell.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

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
	public synchronized boolean requestStop() {
		if (state == RunState.RUNNING) {
			state = RunState.REQUEST_STOP;
			return true;
		} else {
			return false;
		}
	}

	public synchronized void stop(Exception error) throws Exception {
		if (this.state == RunState.STOPPED) {
			LOGGER.debug("Stop() called multiple times");
			return;
		}
		this.state = RunState.REQUEST_STOP;

		LOGGER.info("Stopping " + tasks.size() + " tasks");

		if (error != null) {
			LOGGER.error("cause: ", error);
		}

		// tell everything to stop
		for (StoppableTask task: this.tasks) {
			LOGGER.info("Stopping: " + task);
			task.requestStop();
		}

		// then wait for everything to stop
		Long timeout = 1000L;
		for (StoppableTask task: this.tasks) {
			LOGGER.debug("Awaiting stop of: " + task);
			task.awaitStop(timeout);
		}

		this.state = RunState.STOPPED;
		LOGGER.info("Stopped all tasks");
	}

	public synchronized void add(StoppableTask task) {
		synchronized (tasks) {
			tasks.add(task);
		}
	}
}
