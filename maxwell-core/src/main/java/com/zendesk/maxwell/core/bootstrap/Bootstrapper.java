package com.zendesk.maxwell.core.bootstrap;

import com.zendesk.maxwell.core.producer.AbstractProducer;
import com.zendesk.maxwell.core.replication.Replicator;
import com.zendesk.maxwell.core.row.RowMap;

import java.io.IOException;
import java.sql.SQLException;

public interface Bootstrapper {
	boolean shouldSkip(RowMap row) throws SQLException, IOException;
	void resume(AbstractProducer producer, Replicator replicator) throws Exception;
	boolean isRunning();
	void work(RowMap row, AbstractProducer producer, Replicator replicator) throws Exception;
}
