package com.zendesk.maxwell.producer.partitioners;

public enum PartitionBy {
	DATABASE,
	TABLE,
	PRIMARY_KEY,
	COLUMN,
	FIRST_COLUMN,
	TRANSACTION_ID,
	RANDOM
}
