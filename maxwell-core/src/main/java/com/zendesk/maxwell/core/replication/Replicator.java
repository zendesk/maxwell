package com.zendesk.maxwell.core.replication;

import com.zendesk.maxwell.core.config.MaxwellFilter;
import com.zendesk.maxwell.core.row.RowMap;
import com.zendesk.maxwell.core.schema.Schema;
import com.zendesk.maxwell.core.schema.SchemaStoreException;
import com.zendesk.maxwell.core.util.StoppableTask;

/**
 * Created by ben on 10/23/16.
 */
public interface Replicator extends StoppableTask {
	void setFilter(MaxwellFilter filter);
	void startReplicator() throws Exception;
	RowMap getRow() throws Exception;
	Long getLastHeartbeatRead();
	Schema getSchema() throws SchemaStoreException;

	void stopAtHeartbeat(long heartbeat);
	void runLoop() throws Exception;
}
