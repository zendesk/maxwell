package com.zendesk.maxwell.row;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by jensgeyti on 08/02/2018.
 */
public class FieldNames {
	public static final String COMMIT = "commit";
	public static final String DATA = "data";
	public static final String DATABASE = "database";
	public static final String GTID = "gtid";
	public static final String OLD = "old";
	public static final String POSITION = "position";
	public static final String QUERY = "query";
	public static final String SERVER_ID = "server_id";
	public static final String TABLE = "table";
	public static final String THREAD_ID = "thread_id";
	public static final String SCHEMA_ID = "schema_id";
	public static final String TIMESTAMP = "ts";
	public static final String TRANSACTION_ID = "xid";
	public static final String TRANSACTION_OFFSET = "xoffset";
	public static final String TYPE = "type";
	public static final String UUID = "_uuid";
	public static final String REASON = "reason";

	private static List<String> fieldNamesList = Arrays.asList(COMMIT, DATA, DATABASE,
	GTID, OLD, POSITION, QUERY, SERVER_ID, TABLE, THREAD_ID, TIMESTAMP, TRANSACTION_ID, TYPE, UUID);

	private static final Set<String> fieldNamesSet = new HashSet<>(fieldNamesList);

	public static boolean isProtected(String key) {
		return fieldNamesSet.contains(key);
	}

	public static List<String> getFieldnames() {
		return fieldNamesList;
	}

}
