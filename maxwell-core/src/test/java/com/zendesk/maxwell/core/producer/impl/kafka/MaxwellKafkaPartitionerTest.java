package com.zendesk.maxwell.core.producer.impl.kafka;

import com.zendesk.maxwell.core.row.RowMap;
import com.zendesk.maxwell.core.schema.ddl.DDLMap;
import com.zendesk.maxwell.core.schema.ddl.ResolvedDatabaseAlter;
import com.zendesk.maxwell.core.schema.ddl.ResolvedTableDrop;
import org.junit.Test;

import java.util.ArrayList;

import static org.junit.Assert.assertEquals;

public class MaxwellKafkaPartitionerTest {
	@Test
	public void testRowMapEqualsDDLPartitioning() {
		RowMap r = new RowMap("insert", "db", "tbl", 0L, new ArrayList<>(), null);
		ResolvedDatabaseAlter m = new ResolvedDatabaseAlter("db", "utf8");
		DDLMap d = new DDLMap(m, 0L, "alter-sql", null);

		MaxwellKafkaPartitioner p = new MaxwellKafkaPartitioner("murmur3", "database", null, null);
		assertEquals(p.kafkaPartition(r, 15), p.kafkaPartition(d, 15));
	}

	@Test
	public void testDDLFallBack() {
		ResolvedDatabaseAlter m = new ResolvedDatabaseAlter("some_db", "utf8");
		DDLMap d = new DDLMap(m, 0L, "alter-sql", null);
		MaxwellKafkaPartitioner p = new MaxwellKafkaPartitioner("murmur3", "table", null, "database");

		ResolvedTableDrop m2 = new ResolvedTableDrop("some_db", "some_table");
		DDLMap d2 = new DDLMap(m2, 0L, "alter-sql", null);
		MaxwellKafkaPartitioner p2 = new MaxwellKafkaPartitioner("murmur3", "database", null, "database");

		assertEquals(p2.kafkaPartition(d2, 15), p.kafkaPartition(d, 15));
	}
}
