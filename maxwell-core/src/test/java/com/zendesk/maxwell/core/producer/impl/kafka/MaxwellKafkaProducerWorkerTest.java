package com.zendesk.maxwell.core.producer.impl.kafka;

import com.zendesk.maxwell.core.MaxwellContext;
import com.zendesk.maxwell.core.SpringTestContextConfiguration;
import com.zendesk.maxwell.core.config.MaxwellConfig;
import com.zendesk.maxwell.core.config.MaxwellConfigFactory;
import com.zendesk.maxwell.core.monitoring.NoOpMetrics;
import com.zendesk.maxwell.core.producer.impl.kafka.MaxwellKafkaProducerWorker;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.Properties;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = { SpringTestContextConfiguration.class })
public class MaxwellKafkaProducerWorkerTest {

	@Autowired
	private MaxwellConfigFactory maxwellConfigFactory;

	@Test
	public void constructNewWorkerWithNullTopic() {
		MaxwellContext context = mock(MaxwellContext.class);
		MaxwellConfig config = maxwellConfigFactory.createNewDefaultConfiguration();
		when(context.getConfig()).thenReturn(config);
		when(context.getMetrics()).thenReturn(new NoOpMetrics());
		Properties kafkaProperties = new Properties();
		kafkaProperties.put("bootstrap.servers", "localhost:9092");
		String kafkaTopic = null;
		//shouldn't throw NPE
		new MaxwellKafkaProducerWorker(context, kafkaProperties, kafkaTopic, null);
	}
}