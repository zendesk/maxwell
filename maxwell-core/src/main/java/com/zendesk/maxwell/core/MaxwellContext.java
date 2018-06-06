package com.zendesk.maxwell.core;

import com.zendesk.maxwell.api.config.MaxwellConfig;
import com.zendesk.maxwell.api.replication.Position;
import com.zendesk.maxwell.api.monitoring.MaxwellDiagnosticContext;
import com.zendesk.maxwell.api.monitoring.Metrics;
import com.zendesk.maxwell.core.producer.Producer;
import com.zendesk.maxwell.core.producer.ProducerContext;
import com.zendesk.maxwell.api.row.RowMap;
import com.zendesk.maxwell.core.util.StoppableTask;

import java.sql.SQLException;

public interface MaxwellContext {
	MaxwellConfig getConfig();

	void addTask(StoppableTask task);

	Thread terminate();

	Thread terminate(Exception error);

	void setPosition(RowMap r);

	void setPosition(Position position);

	Position getPosition() throws SQLException;

	Metrics getMetrics();

	MaxwellDiagnosticContext getDiagnosticContext();

	Producer getProducer();

	ProducerContext getProducerContext();
}
