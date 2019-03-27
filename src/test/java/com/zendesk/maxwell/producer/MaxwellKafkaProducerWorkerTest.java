package com.zendesk.maxwell.producer;

import com.zendesk.maxwell.MaxwellConfig;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.monitoring.NoOpMetrics;
import com.zendesk.maxwell.row.RowIdentity;
import org.apache.kafka.clients.producer.Producer;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.stubbing.Answer;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isNotNull;
import static org.mockito.Mockito.*;

public class MaxwellKafkaProducerWorkerTest {
	static class VolatileRef<T> {
		public volatile T value;
	}

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
	public void performsFallbackPublishOnAnotherThread() throws InterruptedException {
		MaxwellContext context = mock(MaxwellContext.class);
		when(context.getConfig()).thenReturn(config);
		when(context.getMetrics()).thenReturn(new NoOpMetrics());
		MaxwellConfig config = new MaxwellConfig();
		Producer<String,String> producer = (Producer<String,String>) mock(Producer.class);
		KafkaCallback callback = mock(KafkaCallback.class);
		String kafkaTopic = "maxwell";
		RowIdentity rowId = new RowIdentity("MyDatabase", "MyTable", Collections.emptyList());
		MaxwellKafkaProducerWorker worker = new MaxwellKafkaProducerWorker(context, kafkaTopic, null, producer);

		Thread thisThread = Thread.currentThread();
		VolatileRef<Thread> publishThread = new VolatileRef<>();
		CountDownLatch ready = new CountDownLatch(1);

		when(producer.send(any(), any())).thenAnswer((Answer<Void>) invocationOnMock -> {
			publishThread.value = Thread.currentThread();
			ready.countDown();
			return null;
		});

		worker.sendFallbackAsync("maxwell.errors", rowId, callback, null, new Exception("The broker is grumpy"));
		verify(callback, never()).onCompletion(any(), isNotNull());

		Assert.assertTrue(ready.await(5L, TimeUnit.SECONDS));
		Assert.assertNotNull(publishThread.value);
		Assert.assertNotEquals(thisThread, publishThread.value);
		worker.close();
	}
}
