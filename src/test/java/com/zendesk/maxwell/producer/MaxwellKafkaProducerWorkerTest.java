package com.zendesk.maxwell.producer;

import com.zendesk.maxwell.MaxwellConfig;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.metrics.NoOpMetrics;
import org.junit.Test;

import java.util.Properties;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MaxwellKafkaProducerWorkerTest {

	@Test
	public void constructNewWorkerWithNullTopic() {
		MaxwellConfig config = new MaxwellConfig();
		config.getKafkaProperties().put("bootstrap.servers", "localhost:9092");
		config.kafkaTopic = null;
		//shouldn't throw NPE
		new MaxwellKafkaProducerWorker(new NoOpMetrics(), config, null);
	}
}
