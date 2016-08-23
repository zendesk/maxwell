package com.zendesk.maxwell.producer.partitioners;

import com.zendesk.maxwell.RowMap;
import org.apache.kafka.common.metrics.stats.Max;

import java.io.IOException;

/**
 * Created by kaufmannkr on 1/18/16.
 */
public class MaxwellKafkaPartitioner {
	HashStringProvider provider;
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
			case "table": this.provider = new HashStringTable();
				break;
			case "primary_key": this.provider = new HashStringPrimaryKey();
				break;
			case "database":
			default:
				this.provider = new HashStringDatabase();
				break;
		}
	}

	static protected String getDatabase(RowMap r) {
		return r.getDatabase();
	}

	static protected String getTable(RowMap r) {
		return r.getTable();
	}

	static protected String getPrimaryKey(RowMap r) {
		return r.pkAsConcatString();
	}

	public int kafkaPartition(RowMap r, int numPartitions) {
		return Math.abs(hashFunc.hashCode(provider.getHashString(r)) % numPartitions);
	}
}
