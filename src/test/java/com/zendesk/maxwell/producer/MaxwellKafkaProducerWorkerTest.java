package com.zendesk.maxwell.producer;

import com.zendesk.maxwell.MaxwellConfig;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.monitoring.NoOpMetrics;
import com.zendesk.maxwell.row.RowIdentity;
import org.apache.kafka.clients.producer.Producer;
import org.junit.Test;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class MaxwellKafkaProducerWorkerTest {
	@Test
	public void constructNewWorkerWithNullTopic() throws TimeoutException {
		MaxwellContext context = mock(MaxwellContext.class);
		MaxwellConfig config = new MaxwellConfig();
		when(context.getConfig()).thenReturn(config);
		when(context.getMetrics()).thenReturn(new NoOpMetrics());
		Properties kafkaProperties = new Properties();
		kafkaProperties.put("bootstrap.servers", "localhost:9092");
		String kafkaTopic = null;
		//shouldn't throw NPE
		MaxwellKafkaProducerWorker worker = new MaxwellKafkaProducerWorker(context, kafkaProperties, kafkaTopic, null);
		worker.close();
	}

	@Test
	public void fallbackPublishIsDeferred() {
		MaxwellContext context = mock(MaxwellContext.class);
		MaxwellConfig config = new MaxwellConfig();
		when(context.getConfig()).thenReturn(config);
		config.deadLetterTopic = "maxwell.errors";
		when(context.getMetrics()).thenReturn(new NoOpMetrics());
		Producer<String,String> producer = (Producer<String,String>) mock(Producer.class);
		KafkaCallback callback = mock(KafkaCallback.class);
		String kafkaTopic = "maxwell";
		RowIdentity rowId = new RowIdentity("MyDatabase", "MyTable", "insert", Collections.emptyList());
		MaxwellKafkaProducerWorker worker = new MaxwellKafkaProducerWorker(context, kafkaTopic, null, producer);

		worker.enqueueFallbackRow("maxwell.errors", rowId, callback, null, new Exception("The broker is grumpy"));
		verify(callback, never()).onCompletion(any(), isNotNull());
		verify(producer, never()).send(any(), any());

		worker.drainDeadLetterQueue();
		verify(producer, times(1)).send(any(), any());
		worker.close();
	}
}
