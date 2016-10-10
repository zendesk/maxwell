package com.zendesk.maxwell.producer.partitioners;

import com.zendesk.maxwell.RowMap;
import org.apache.kafka.common.metrics.stats.Max;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by kaufmannkr on 1/18/16.
 */
public class MaxwellKafkaPartitioner {
	HashStringProvider provider;
	HashFunction hashFunc;
	List<String> partitionColumns = new ArrayList<String>();

	private String partitionKeyFallback;

	public MaxwellKafkaPartitioner(String hashFunction, String partitionKey, String csvColumns, String partitionKeyFallback) {
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
			case "column": this.provider = new HashStringColumn();
				break;
			case "database":
			default:
				this.provider = new HashStringDatabase();
				break;
		}
		if ( partitionColumns != null )
			this.partitionColumns = Arrays.asList(csvColumns.split(","));

		this.partitionKeyFallback = partitionKeyFallback;
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
		return Math.abs(hashFunc.hashCode(provider.getHashString(r, this.partitionColumns, this.partitionKeyFallback)) % numPartitions);
	}
}
