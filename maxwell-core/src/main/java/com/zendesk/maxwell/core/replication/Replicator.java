package com.zendesk.maxwell.core.replication;

import com.zendesk.maxwell.api.StoppableTask;
import com.zendesk.maxwell.api.config.MaxwellFilter;
import com.zendesk.maxwell.api.row.RowMap;
import com.zendesk.maxwell.core.schema.Schema;
import com.zendesk.maxwell.core.schema.SchemaStoreException;

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
