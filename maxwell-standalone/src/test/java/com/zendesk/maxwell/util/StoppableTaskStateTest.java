package com.zendesk.maxwell.util;

import org.junit.Test;

import java.util.concurrent.TimeoutException;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

public class StoppableTaskStateTest {

	@Test
	public void testStateTransition() {
		StoppableTaskState state = new StoppableTaskState("task");
		assertThat(state.getState(), equalTo(RunState.RUNNING));
		assertThat(state.isRunning(), equalTo(true));

		state.requestStop();
		assertThat(state.getState(), equalTo(RunState.REQUEST_STOP));
		assertThat(state.isRunning(), equalTo(false));

		state.stopped();
		assertThat(state.getState(), equalTo(RunState.STOPPED));
		assertThat(state.isRunning(), equalTo(false));
	}

	@Test
	public void requestStopIsIgnoredIfAlreadyStopped() {
		StoppableTaskState state = new StoppableTaskState("task");
		state.stopped();

		state.requestStop();
		assertThat(state.getState(), equalTo(RunState.STOPPED));
	}

	@Test
	public void awaitSucceedsWhenAlreadyStopped() throws TimeoutException {
		StoppableTaskState state = new StoppableTaskState("task");
		state.stopped();

		state.awaitStop(null, 0L);
	}

	@Test
	public void awaitThrowsWhenNotStopped() {
		StoppableTaskState state = new StoppableTaskState("task");

		TimeoutException e = null;
		try {
			state.awaitStop(null, 0L);
		} catch (TimeoutException _e) {
			e = _e;
		}

		assertThat(e, notNullValue());
		assertThat(state.getState(), equalTo(RunState.RUNNING));
	}
}
