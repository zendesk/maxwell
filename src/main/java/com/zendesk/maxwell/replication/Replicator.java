package com.zendesk.maxwell.replication;

import com.zendesk.maxwell.filtering.FilterV2;
import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.schema.SchemaStoreException;
import com.zendesk.maxwell.schema.Schema;
import com.zendesk.maxwell.util.StoppableTask;

/**
 * Created by ben on 10/23/16.
 */
public interface Replicator extends StoppableTask {
	void setFilter(FilterV2 filter);
	void startReplicator() throws Exception;
	RowMap getRow() throws Exception;
	Long getLastHeartbeatRead();
	Schema getSchema() throws SchemaStoreException;

	void stopAtHeartbeat(long heartbeat);
	void runLoop() throws Exception;
}
