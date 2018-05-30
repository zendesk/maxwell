package com.zendesk.maxwell.core.producer;

import com.codahale.metrics.Meter;
import com.zendesk.maxwell.core.config.Extension;
import com.zendesk.maxwell.core.monitoring.MaxwellDiagnostic;
import com.zendesk.maxwell.core.row.RowMap;
import com.zendesk.maxwell.core.util.StoppableTask;

public interface Producer extends Extension {
	void push(RowMap r) throws Exception;
	StoppableTask getStoppableTask();
	MaxwellDiagnostic getDiagnostic();
	Meter getFailedMessageMeter();
}
