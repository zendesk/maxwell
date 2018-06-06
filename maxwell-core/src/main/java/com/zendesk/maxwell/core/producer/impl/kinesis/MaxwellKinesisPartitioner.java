package com.zendesk.maxwell.core.producer.impl.kinesis;

import com.zendesk.maxwell.core.producer.partitioners.AbstractMaxwellPartitioner;
import com.zendesk.maxwell.api.row.RowMap;
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
