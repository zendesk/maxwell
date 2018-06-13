package com.zendesk.maxwell.producer.partitioners;

import com.zendesk.maxwell.row.RowMap;

import java.util.HashMap;
import java.util.Map;

public class MaxwellKafkaDBNamePartitioner extends AbstractMaxwellKafkaPartitioner {
	private Map<String, Integer> kafkaPartitionMap;

	public MaxwellKafkaDBNamePartitioner(Map<String, Integer> kafkaPartitionMap) {
		super(null, null, null);
		this.kafkaPartitionMap = kafkaPartitionMap;
	}

	@Override
	public int kafkaPartition(RowMap r, int numPartitions) {
		return kafkaPartitionMap.get(r.getDatabase());
	}
}
