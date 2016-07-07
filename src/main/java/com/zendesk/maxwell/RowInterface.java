package com.zendesk.maxwell;

import com.zendesk.maxwell.producer.partitioners.PartitionKeyType;
import java.io.IOException;

public interface RowInterface {
	public enum KeyFormat { HASH, ARRAY }

	public String rowKey(KeyFormat key) throws IOException;
	public String toJSON() throws IOException;
	public boolean isTXCommit();
	public BinlogPosition getPosition();
	public String getPartitionKey(PartitionKeyType keyType);
}
