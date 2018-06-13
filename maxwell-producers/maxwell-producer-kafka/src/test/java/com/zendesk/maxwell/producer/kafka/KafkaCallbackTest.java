package com.zendesk.maxwell.producer.kafka;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.zendesk.maxwell.core.MaxwellContext;
import com.zendesk.maxwell.api.config.MaxwellConfig;
import com.zendesk.maxwell.api.replication.BinlogPosition;
import com.zendesk.maxwell.api.replication.Position;
import com.zendesk.maxwell.core.springconfig.CoreComponentScanConfig;
import com.zendesk.maxwell.core.config.BaseMaxwellConfig;
import com.zendesk.maxwell.core.config.MaxwellConfigFactory;
import com.zendesk.maxwell.core.producer.AbstractAsyncProducer;
import com.zendesk.maxwell.test.springconfig.TestSupportComponentScan;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.NotEnoughReplicasException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static org.mockito.Mockito.*;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = { CoreComponentScanConfig.class, TestSupportComponentScan.class })
public class KafkaCallbackTest {

	@Autowired
	private MaxwellConfigFactory maxwellConfigFactory;

	@Test
	public void shouldIgnoreProducerErrorByDefault() {
		MaxwellContext context = mock(MaxwellContext.class);
		MaxwellConfig config = maxwellConfigFactory.create();
		when(context.getConfig()).thenReturn(config);
		AbstractAsyncProducer.CallbackCompleter cc = mock(AbstractAsyncProducer.CallbackCompleter.class);
		KafkaCallback callback = new KafkaCallback(cc,
			new Position(new BinlogPosition(1, "binlog-1"), 0L), "key", "value",
			new Counter(), new Counter(), new Meter(), new Meter(),
			context);
		NotEnoughReplicasException error = new NotEnoughReplicasException("blah");
		callback.onCompletion(new RecordMetadata(new TopicPartition("topic", 1), 1, 1, 1, new Long(1), 1, 1), error);
		verify(cc).markCompleted();
	}

	@Test
	public void shouldTerminateWhenNotIgnoreProducerError() {
		MaxwellContext context = mock(MaxwellContext.class);
		BaseMaxwellConfig config = maxwellConfigFactory.create();
		config.setIgnoreProducerError(false);
		when(context.getConfig()).thenReturn(config);
		AbstractAsyncProducer.CallbackCompleter cc = mock(AbstractAsyncProducer.CallbackCompleter.class);
		KafkaCallback callback = new KafkaCallback(cc,
			new Position(new BinlogPosition(1, "binlog-1"), 0L), "key", "value",
			new Counter(), new Counter(), new Meter(), new Meter(),
			context);
		NotEnoughReplicasException error = new NotEnoughReplicasException("blah");
		callback.onCompletion(new RecordMetadata(new TopicPartition("topic", 1), 1, 1, 1, new Long(1), 1, 1), error);
		verify(context).terminate(error);
		verifyZeroInteractions(cc);
	}
}
