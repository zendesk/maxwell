package com.zendesk.maxwell.producer;

import com.zendesk.maxwell.MaxwellConfig;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.monitoring.NoOpMetrics;
import com.zendesk.maxwell.replication.BinlogPosition;
import com.zendesk.maxwell.replication.Position;
import com.zendesk.maxwell.row.RowMap;
import org.junit.Test;

import java.util.Properties;
import java.util.ArrayList;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MaxwellKinesisProducerTest {

	private static final long TIMESTAMP_MILLISECONDS = 1496712943447L;

	private static final Position POSITION = new Position(new BinlogPosition(1L, "binlog-0001"), 0L);

	@Test
	public void dealsWithTooLargeRecord() throws Exception {
		MaxwellContext context = mock(MaxwellContext.class);
		MaxwellConfig config = new MaxwellConfig();
		when(context.getConfig()).thenReturn(config);
		when(context.getMetrics()).thenReturn(new NoOpMetrics());
		String kinesisStream = "test-stream";
		MaxwellKinesisProducer producer = new MaxwellKinesisProducer(context, kinesisStream);

		RowMap rowMap = new RowMap("insert", "MyDatabase", "MyTable", TIMESTAMP_MILLISECONDS, new ArrayList<String>(), POSITION);
		StringBuilder r = new StringBuilder();
		for (int i = 0; i < 100_000; i++) {
			r.append("long string");
		}
		rowMap.putData("content", r.toString());

		AbstractAsyncProducer.CallbackCompleter cc = mock(AbstractAsyncProducer.CallbackCompleter.class);
		producer.sendAsync(rowMap, cc);
		producer.close();
	}
}
