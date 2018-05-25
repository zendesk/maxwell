package com.zendesk.maxwell.core.producer;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.zendesk.maxwell.core.MaxwellContext;
import com.zendesk.maxwell.core.config.MaxwellConfig;
import com.zendesk.maxwell.core.config.MaxwellConfigFactory;
import com.zendesk.maxwell.core.replication.BinlogPosition;
import com.zendesk.maxwell.core.replication.Position;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.NotEnoughReplicasException;
import org.junit.Test;

import static org.mockito.Mockito.*;

public class KafkaCallbackTest {

	@Test
	public void shouldIgnoreProducerErrorByDefault() {
		MaxwellContext context = mock(MaxwellContext.class);
		MaxwellConfig config = new MaxwellConfigFactory().createNewDefaultConfiguration();
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
		MaxwellConfig config = new MaxwellConfigFactory().createNewDefaultConfiguration();
		config.ignoreProducerError = false;
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
