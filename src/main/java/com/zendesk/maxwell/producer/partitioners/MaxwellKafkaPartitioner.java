package com.zendesk.maxwell.producer.partitioners;

import com.zendesk.maxwell.RowInterface;
import org.apache.kafka.common.metrics.stats.Max;

import java.io.IOException;

/**
 * Created by kaufmannkr on 1/18/16.
 */
public class MaxwellKafkaPartitioner {
	PartitionKeyType partitionKeyType;
	HashFunction hashFunc;
	public MaxwellKafkaPartitioner(String hashFunction, String partitionKey) {
		int MURMUR_HASH_SEED = 25342;
		switch (hashFunction) {
			case "murmur3": this.hashFunc = new HashFunctionMurmur3(MURMUR_HASH_SEED);
				break;
			case "default":
			default:
				this.hashFunc = new HashFunctionDefault();
				break;
		}
		switch (partitionKey) {
			case "table": this.partitionKeyType = PartitionKeyType.TABLE;
				break;
			case "primary_key": this.partitionKeyType = PartitionKeyType.PRIMARY_KEY;
				break;
			case "database":
			default:
				this.partitionKeyType = PartitionKeyType.DATABASE;
				break;
		}
	}

	public int kafkaPartition(RowInterface r, int numPartitions) {
		return Math.abs(hashFunc.hashCode(r.getPartitionKey(this.partitionKeyType)) % numPartitions);
	}
}
