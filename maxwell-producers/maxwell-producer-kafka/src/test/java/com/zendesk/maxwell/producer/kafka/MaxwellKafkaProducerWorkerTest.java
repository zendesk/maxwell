package com.zendesk.maxwell.producer.kafka;

import com.zendesk.maxwell.api.MaxwellContext;
import com.zendesk.maxwell.api.config.MaxwellConfig;
import com.zendesk.maxwell.core.SpringLauncherScanConfig;
import com.zendesk.maxwell.core.config.MaxwellConfigFactory;
import com.zendesk.maxwell.core.monitoring.NoOpMetrics;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = { SpringLauncherScanConfig.class })
public class MaxwellKafkaProducerWorkerTest {

	@Autowired
	private MaxwellConfigFactory maxwellConfigFactory;

	@Test
	public void constructNewWorkerWithNullTopic() {
		MaxwellContext context = mock(MaxwellContext.class);

		KafkaProducerConfiguration configuration = new KafkaProducerConfiguration();
		configuration.getKafkaProperties().put("bootstrap.servers", "localhost:9092");

		MaxwellConfig config = maxwellConfigFactory.create();
		when(context.getConfig()).thenReturn(config);
		when(context.getMetrics()).thenReturn(new NoOpMetrics());

		//shouldn't throw NPE
		new MaxwellKafkaProducerWorker(context, configuration, null);
	}
}
