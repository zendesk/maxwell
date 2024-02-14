/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.zendesk.maxwell.replication.vitess;

/**
 * If a column is part of the primary key (including multi-column primary key),
 * or a column is a single-column unique key
 */
public enum KeyMetaData {
	/**
	 * The column is part of the primary key (including multi-column primary key)
	 */
	IS_KEY,
	/**
	 * The column is single-column unique key
	 */
	IS_UNIQUE_KEY,
	/**
	 * The column is not part of any key, or the column is part of multi-column
	 * unique key
	 */
	NONE
}
