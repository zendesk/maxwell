package com.zendesk.maxwell.producer.partitioners;

import com.zendesk.maxwell.row.RowMap;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class MaxwellKafkaDBNamePartitionerTest {
	@Test
	public void testKafkaPartitionBasedOnPartionMap() {
		Map<String, Integer> partitionMap = new HashMap<>();
		partitionMap.put("db1", 1);
		partitionMap.put("db2", 2);
		MaxwellKafkaDBNamePartitioner p = new MaxwellKafkaDBNamePartitioner(partitionMap);
		int partition = p.kafkaPartition(new RowMap("insert", "db1", "table", 1234L, null, null, null), 1);

		assertEquals(1, partition);
	}
}
