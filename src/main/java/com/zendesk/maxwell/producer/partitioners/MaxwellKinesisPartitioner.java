package com.zendesk.maxwell.producer.partitioners;

import com.zendesk.maxwell.row.RowMap;

import org.apache.commons.codec.digest.DigestUtils;

public class MaxwellKinesisPartitioner extends MaxwellPartitioner {
	boolean hashKey;

	public MaxwellKinesisPartitioner(String partitionKey, String csvPartitionColumns, String partitionKeyFallback, boolean hashKey) {
		super(partitionKey, csvPartitionColumns, partitionKeyFallback);

		this.hashKey = hashKey;
	}

	public String getKinesisKey(RowMap r) {
		String key = this.getHashString(r);

		if(hashKey) {
			return DigestUtils.md5Hex(key);
		}

		return key;
	}
}
