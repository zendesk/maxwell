package com.zendesk.maxwell.util;

import java.util.concurrent.TimeoutException;

public interface StoppableTask {
	public enum StopPriority {
		BINLOG,
		PRODUCER,
		SUPPORT
	}

	void requestStop() throws Exception;
	void awaitStop(Long timeout) throws TimeoutException;
	StopPriority getStopPriority();
}
