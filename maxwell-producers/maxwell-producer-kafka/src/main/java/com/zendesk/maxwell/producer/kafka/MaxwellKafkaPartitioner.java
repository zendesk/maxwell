package com.zendesk.maxwell.producer.kafka;

import com.zendesk.maxwell.api.row.RowMap;
import com.zendesk.maxwell.core.producer.partitioners.AbstractMaxwellPartitioner;
import com.zendesk.maxwell.core.producer.partitioners.HashFunction;
import com.zendesk.maxwell.core.producer.partitioners.HashFunctionDefault;
import com.zendesk.maxwell.core.producer.partitioners.HashFunctionMurmur3;

public class MaxwellKafkaPartitioner extends AbstractMaxwellPartitioner {
	HashFunction hashFunc;

	public MaxwellKafkaPartitioner(String hashFunction, String partitionKey, String csvPartitionColumns, String partitionKeyFallback) {
		super(partitionKey, csvPartitionColumns, partitionKeyFallback);

		int MURMUR_HASH_SEED = 25342;
		switch (hashFunction) {
			case "murmur3": this.hashFunc = new HashFunctionMurmur3(MURMUR_HASH_SEED);
				break;
			case "default":
			default:
				this.hashFunc = new HashFunctionDefault();
				break;
		}
	}

	public int kafkaPartition(RowMap r, int numPartitions) {
		return Math.abs(hashFunc.hashCode(this.getHashString(r)) % numPartitions);
	}
}
