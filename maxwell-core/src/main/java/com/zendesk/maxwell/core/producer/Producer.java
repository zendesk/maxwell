package com.zendesk.maxwell.core.producer;

import com.codahale.metrics.Meter;
import com.zendesk.maxwell.core.monitoring.MaxwellDiagnostic;
import com.zendesk.maxwell.core.row.RowMap;
import com.zendesk.maxwell.core.StoppableTask;

public interface Producer {
	void push(RowMap r) throws Exception;
	StoppableTask getStoppableTask();
	MaxwellDiagnostic getDiagnostic();
	Meter getFailedMessageMeter();
}
