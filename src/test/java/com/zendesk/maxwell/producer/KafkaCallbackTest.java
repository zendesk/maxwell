package com.zendesk.maxwell.producer;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.zendesk.maxwell.MaxwellConfig;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.replication.BinlogPosition;
import com.zendesk.maxwell.replication.Position;
import com.zendesk.maxwell.row.RowIdentity;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.NotEnoughReplicasException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static org.mockito.Mockito.*;

public class KafkaCallbackTest {
	private MaxwellContext makeContext(MaxwellConfig config) {
		MaxwellContext maxwellContext = mock(MaxwellContext.class);
		when(maxwellContext.getConfig()).thenReturn(config);
		return maxwellContext;
	}

	private KafkaCallback makeCallback(
		MaxwellContext context,
		AbstractAsyncProducer.CallbackCompleter cc,
		MaxwellKafkaProducerWorker producer,
		RowIdentity id)
	{
		if (producer == null) {
			producer = mock(MaxwellKafkaProducerWorker.class);
		}
		if (id == null) {
			id = new RowIdentity("a","b", "insert", null);
		}

		Position position = new Position(new BinlogPosition(1, "binlog-1"), 0L);

		return new KafkaCallback(cc, position, id, "value",
				new Counter(), new Counter(), new Meter(), new Meter(),
				"maxwell", context.getConfig().deadLetterTopic, context, producer);
	}

	@Test
	public void shouldIgnoreProducerErrorByDefault() {
		AbstractAsyncProducer.CallbackCompleter cc = mock(AbstractAsyncProducer.CallbackCompleter.class);
		MaxwellContext context = makeContext(new MaxwellConfig());
		KafkaCallback callback = makeCallback(context, cc, null, null);
		Exception error = new NotEnoughReplicasException("blah");
		callback.onCompletion(new RecordMetadata(new TopicPartition("topic", 1), 1, 1, 1, new Long(1), 1, 1), error);
		verify(cc).markCompleted();
	}

	@Test
	public void shouldTerminateWhenNotIgnoringProducerError() {
		MaxwellConfig config = new MaxwellConfig();
		config.ignoreProducerError = false;
		MaxwellContext context = makeContext(config);
		AbstractAsyncProducer.CallbackCompleter cc = mock(AbstractAsyncProducer.CallbackCompleter.class);
		MaxwellKafkaProducerWorker producer = mock(MaxwellKafkaProducerWorker.class);
		KafkaCallback callback = makeCallback(context, cc, producer, null);

		Exception error = new NotEnoughReplicasException("blah");
		callback.onCompletion(new RecordMetadata(new TopicPartition("topic", 1), 1, 1, 1, new Long(1), 1, 1), error);
		verify(context).terminate(error);
		verifyZeroInteractions(producer);
		verifyZeroInteractions(cc);
	}

	@Test
	public void shouldPublishFallbackRecordOnRecordTooLargeWhenConfigured() throws Exception {
		MaxwellConfig config = new MaxwellConfig();
		config.deadLetterTopic = "dead_letters";
		MaxwellContext context = makeContext(config);
		MaxwellKafkaProducerWorker producer = mock(MaxwellKafkaProducerWorker.class);
		AbstractAsyncProducer.CallbackCompleter cc = mock(AbstractAsyncProducer.CallbackCompleter.class);
		RowIdentity id = new RowIdentity("a","b", "insert", null);
		KafkaCallback callback = makeCallback(context, cc, producer, id);

		Exception error = new RecordTooLargeException();
		RecordMetadata recordMetadata = new RecordMetadata(new TopicPartition("topic", 1), 1, 1, 1, new Long(1), 1, 1);
		callback.onCompletion(recordMetadata, error);

		// don't complete yet!
		verifyZeroInteractions(cc);

		ArgumentCaptor<KafkaCallback> cbCaptor = ArgumentCaptor.forClass(KafkaCallback.class);
		verify(producer).enqueueFallbackRow(eq("dead_letters"), eq(id), cbCaptor.capture(), any(), eq(error));
		Assert.assertEquals(null, cbCaptor.getValue().getFallbackTopic());
		cbCaptor.getValue().onCompletion(recordMetadata, null);
		verify(cc).markCompleted();
	}

	@Test
	public void shouldPublishFallbackRecordOnRetriableExceptionWhenConfiguredWithFallbackAndIgnoreErrors() throws Exception {
		MaxwellConfig config = new MaxwellConfig();
		config.deadLetterTopic = "dead_letters";
		config.ignoreProducerError = true;
		MaxwellContext context = makeContext(config);
		MaxwellKafkaProducerWorker producer = mock(MaxwellKafkaProducerWorker.class);
		AbstractAsyncProducer.CallbackCompleter cc = mock(AbstractAsyncProducer.CallbackCompleter.class);
		RowIdentity id = new RowIdentity("a","b", "insert", null);

		KafkaCallback callback = makeCallback(context, cc, producer, id);

		Exception error = new NotEnoughReplicasException("blah");
		RecordMetadata recordMetadata = new RecordMetadata(new TopicPartition("topic", 1), 1, 1, 1, new Long(1), 1, 1);
		callback.onCompletion(recordMetadata, error);

		// don't complete yet!
		verifyZeroInteractions(cc);

		ArgumentCaptor<KafkaCallback> cbCaptor = ArgumentCaptor.forClass(KafkaCallback.class);
		verify(producer).enqueueFallbackRow(eq("dead_letters"), eq(id), cbCaptor.capture(), any(), eq(error));
		Assert.assertEquals(null, cbCaptor.getValue().getFallbackTopic());
		cbCaptor.getValue().onCompletion(recordMetadata, null);
		verify(cc).markCompleted();
	}

	@Test
	public void shouldNotPublishFallbackRecordIfNotConfigured() {
		MaxwellConfig config = new MaxwellConfig();
		config.deadLetterTopic = null;
		MaxwellContext context = makeContext(config);
		MaxwellKafkaProducerWorker producer = mock(MaxwellKafkaProducerWorker.class);
		AbstractAsyncProducer.CallbackCompleter cc = mock(AbstractAsyncProducer.CallbackCompleter.class);
		RowIdentity id = new RowIdentity("a","b", "insert", null);
		KafkaCallback callback = makeCallback(context, cc, producer, id);

		Exception error = new RecordTooLargeException();
		RecordMetadata recordMetadata = new RecordMetadata(new TopicPartition("topic", 1), 1, 1, 1, new Long(1), 1, 1);
		callback.onCompletion(recordMetadata, error);

		verify(cc).markCompleted();
		verifyZeroInteractions(producer);
	}
}

