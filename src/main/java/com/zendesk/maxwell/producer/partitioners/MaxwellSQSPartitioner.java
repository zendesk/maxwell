package com.zendesk.maxwell.producer.partitioners;

import com.zendesk.maxwell.row.RowMap;

public class MaxwellSQSPartitioner extends AbstractMaxwellPartitioner {

    public MaxwellSQSPartitioner(String partitionKey, String csvPartitionColumns, String partitionKeyFallback) {
        super(partitionKey, csvPartitionColumns, partitionKeyFallback);
    }

    public String getSQSKey(RowMap r) {
        String key = this.getHashString(r);
        return key;
    }
}