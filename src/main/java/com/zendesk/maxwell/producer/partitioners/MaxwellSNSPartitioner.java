package com.zendesk.maxwell.producer.partitioners;

import com.zendesk.maxwell.row.RowMap;

public class MaxwellSNSPartitioner extends AbstractMaxwellPartitioner {

    public MaxwellSNSPartitioner(String partitionKey, String csvPartitionColumns, String partitionKeyFallback) {
        super(partitionKey, csvPartitionColumns, partitionKeyFallback);
    }

    public String getSNSKey(RowMap r) {
        String key = this.getHashString(r);
        return key;
    }
}