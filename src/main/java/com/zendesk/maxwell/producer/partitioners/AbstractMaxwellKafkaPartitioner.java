package com.zendesk.maxwell.producer.partitioners;

import com.zendesk.maxwell.row.RowMap;

public abstract class AbstractMaxwellKafkaPartitioner extends AbstractMaxwellPartitioner {

	public AbstractMaxwellKafkaPartitioner(String partitionKey, String csvPartitionColumns, String partitionKeyFallback) {
		super(partitionKey, csvPartitionColumns, partitionKeyFallback);
	}

	public abstract int kafkaPartition(RowMap r, int numPartitions);
}
