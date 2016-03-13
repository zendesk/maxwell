package com.zendesk.maxwell;

import com.zendesk.maxwell.producer.partitioners.PartitionKeyType;
import java.io.IOException;

public interface RowInterface {
	public String rowKey() throws IOException;
	public String toJSON() throws IOException;
	public boolean isTXCommit();
	public BinlogPosition getPosition();
	public String getPartitionKey(PartitionKeyType keyType);
}
