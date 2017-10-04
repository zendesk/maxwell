package com.zendesk.maxwell.util;

import org.apache.commons.lang3.tuple.MutablePair;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

public class TaskManagerTest {
	class SampleTask implements StoppableTask {
		private final List<Event> log;
		public String name;

		SampleTask(List<Event> eventLog, String name) {
			this.log = eventLog;
			this.name = name;
		}

		@Override
		public void requestStop() {
			log.add(new Event(EventType.REQUEST_STOP, this.name));
		}

		@Override
		public void awaitStop(Long timeout) throws TimeoutException {
			log.add(new Event(EventType.AWAIT_STOP, this.name));
		}
	}
	enum EventType { REQUEST_STOP, AWAIT_STOP };

	class UnstoppableTask implements StoppableTask {
		UnstoppableTask() {
		}

		@Override
		public void requestStop() {
		}

		@Override
		public void awaitStop(Long timeout) throws TimeoutException {
			throw new TimeoutException("can't stop this");
		}
	}

	class Event extends MutablePair<EventType, String> {
		Event(EventType left, String right) {
			super(left, right);
		}
	}

	@Test
	public void shutsDownAllTasksAndWaitsForCompletion() throws Exception {
		List<Event> log = new ArrayList<>();
		StoppableTask task1 = new SampleTask(log, "task1");
		StoppableTask task2 = new SampleTask(log, "task2");

		TaskManager manager = new TaskManager();
		manager.add(task1);
		manager.add(task2);

		manager.stop(null);
		assertThat(log, equalTo(Arrays.asList(
			// stop tasks first
			new Event(EventType.REQUEST_STOP, "task1"),
			new Event(EventType.REQUEST_STOP, "task2"),
			// ... then wait
			new Event(EventType.AWAIT_STOP, "task1"),
			new Event(EventType.AWAIT_STOP, "task2")
		)));
	}

	@Test
	public void raisesOnAwaitTimeout() throws Exception {
		List<Event> log = new ArrayList<>();
		StoppableTask task1 = new UnstoppableTask();

		TaskManager manager = new TaskManager();
		manager.add(task1);

		TimeoutException timeout = null;
		try {
			manager.stop(null);
		} catch(TimeoutException e) {
			timeout = e;
		}
		assertThat(timeout, notNullValue());
	}

	@Test
	public void onlyStopsTasksOnce() throws Exception {
		List<Event> log = new ArrayList<>();
		StoppableTask task = new SampleTask(log, "task");
		TaskManager manager = new TaskManager();
		manager.add(task);

		assertThat(manager.requestStop(), equalTo(true));
		assertThat(manager.requestStop(), equalTo(false));
		manager.stop(null);
		manager.stop(null);

		assertThat(log, equalTo(Arrays.asList(
			new Event(EventType.REQUEST_STOP, "task"),
			new Event(EventType.AWAIT_STOP, "task")
		)));
	}
}
