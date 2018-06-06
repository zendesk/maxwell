package com.zendesk.maxwell.api.producer;

import com.codahale.metrics.Meter;
import com.zendesk.maxwell.api.StoppableTask;
import com.zendesk.maxwell.api.monitoring.MaxwellDiagnostic;
import com.zendesk.maxwell.api.row.RowMap;

public interface Producer {
	void push(RowMap r) throws Exception;
	StoppableTask getStoppableTask();
	MaxwellDiagnostic getDiagnostic();
	Meter getFailedMessageMeter();
}
