package com.zendesk.maxwell.producer.partitioners;

import com.zendesk.maxwell.row.RowMap;

import org.apache.commons.codec.digest.DigestUtils;

public class MaxwellKinesisPartitioner extends AbstractMaxwellPartitioner {
	boolean md5Keys;

	public MaxwellKinesisPartitioner(String partitionKey, String csvPartitionColumns, String partitionKeyFallback, boolean md5Keys) {
		super(partitionKey, csvPartitionColumns, partitionKeyFallback);

		this.md5Keys = md5Keys;
	}

	public String getKinesisKey(RowMap r) {
		String key = this.getHashString(r);

		if(md5Keys) {
			return DigestUtils.md5Hex(key);
		}

		return key;
	}
}
