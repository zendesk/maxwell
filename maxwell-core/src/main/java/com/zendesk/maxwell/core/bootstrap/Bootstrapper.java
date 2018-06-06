package com.zendesk.maxwell.core.bootstrap;

import com.zendesk.maxwell.api.producer.Producer;
import com.zendesk.maxwell.api.row.RowMap;
import com.zendesk.maxwell.core.replication.Replicator;

import java.io.IOException;
import java.sql.SQLException;

public interface Bootstrapper {
	boolean shouldSkip(RowMap row) throws SQLException, IOException;
	void resume(Producer producer, Replicator replicator) throws Exception;
	boolean isRunning();
	void work(RowMap row, Producer producer, Replicator replicator) throws Exception;
}
