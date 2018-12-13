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
	private KafkaCallback.ProducerContext makeContext() {
		MaxwellContext maxwellContext = mock(MaxwellContext.class);
		MaxwellConfig config = new MaxwellConfig();
		when(maxwellContext.getConfig()).thenReturn(config);
		MaxwellKafkaProducerWorker producer = mock(MaxwellKafkaProducerWorker.class);

		return new KafkaCallback.ProducerContext(producer, maxwellContext, null,
			new Counter(), new Counter(), new Meter(), new Meter());
	}

	@Test
	public void shouldIgnoreProducerErrorByDefault() {
		AbstractAsyncProducer.CallbackCompleter cc = mock(AbstractAsyncProducer.CallbackCompleter.class);
		KafkaCallback.ProducerContext context = makeContext();
		KafkaCallback callback = new KafkaCallback(cc,
			new Position(new BinlogPosition(1, "binlog-1"), 0L),
			new RowIdentity("a","b", null), "value",
			context);
		Exception error = new NotEnoughReplicasException("blah");
		callback.onCompletion(new RecordMetadata(new TopicPartition("topic", 1), 1, 1, 1, new Long(1), 1, 1), error);
		verify(cc).markCompleted();
	}

	@Test
	public void shouldTerminateWhenNotIgnoringProducerError() {
		KafkaCallback.ProducerContext context = makeContext();
		context.maxwell.getConfig().ignoreProducerError = false;

		AbstractAsyncProducer.CallbackCompleter cc = mock(AbstractAsyncProducer.CallbackCompleter.class);
		KafkaCallback callback = new KafkaCallback(cc,
			new Position(new BinlogPosition(1, "binlog-1"), 0L), new RowIdentity("a","b", null), "value",
			context);

		Exception error = new NotEnoughReplicasException("blah");
		callback.onCompletion(new RecordMetadata(new TopicPartition("topic", 1), 1, 1, 1, new Long(1), 1, 1), error);
		verify(context.maxwell).terminate(error);
		verifyZeroInteractions(context.producer);
		verifyZeroInteractions(cc);
	}

	@Test
	public void shouldPublishFallbackRecordOnRecordTooLargeWhenConfigured() throws Exception {
		KafkaCallback.ProducerContext context = makeContext();
		context.fallbackTopic = "dead_letters";

		AbstractAsyncProducer.CallbackCompleter cc = mock(AbstractAsyncProducer.CallbackCompleter.class);
		RowIdentity id = new RowIdentity("a","b", null);
		KafkaCallback callback = new KafkaCallback(cc,
			new Position(new BinlogPosition(1, "binlog-1"), 0L), id, "value",
			context);

		Exception error = new RecordTooLargeException();
		RecordMetadata recordMetadata = new RecordMetadata(new TopicPartition("topic", 1), 1, 1, 1, new Long(1), 1, 1);
		callback.onCompletion(recordMetadata, error);

		// don't complete yet!
		verifyZeroInteractions(cc);

		ArgumentCaptor<KafkaCallback> cbCaptor = ArgumentCaptor.forClass(KafkaCallback.class);
		verify(context.producer).sendFallbackAsync(eq("dead_letters"), eq(id), cbCaptor.capture(), eq(error));
		Assert.assertEquals(null, cbCaptor.getValue().getContext().fallbackTopic);
		cbCaptor.getValue().onCompletion(recordMetadata, null);
		verify(cc).markCompleted();
	}

	@Test
	public void shouldPublishFallbackRecordOnRetriableExceptionWhenConfiguredWithFallbackAndIgnoreErrors() throws Exception {
		KafkaCallback.ProducerContext context = makeContext();
		context.fallbackTopic = "dead_letters";
		context.maxwell.getConfig().ignoreProducerError = true;

		AbstractAsyncProducer.CallbackCompleter cc = mock(AbstractAsyncProducer.CallbackCompleter.class);
		RowIdentity id = new RowIdentity("a","b", null);
		KafkaCallback callback = new KafkaCallback(cc,
				new Position(new BinlogPosition(1, "binlog-1"), 0L), id, "value",
				context);

		Exception error = new NotEnoughReplicasException("blah");
		RecordMetadata recordMetadata = new RecordMetadata(new TopicPartition("topic", 1), 1, 1, 1, new Long(1), 1, 1);
		callback.onCompletion(recordMetadata, error);

		// don't complete yet!
		verifyZeroInteractions(cc);

		ArgumentCaptor<KafkaCallback> cbCaptor = ArgumentCaptor.forClass(KafkaCallback.class);
		verify(context.producer).sendFallbackAsync(eq("dead_letters"), eq(id), cbCaptor.capture(), eq(error));
		Assert.assertEquals(null, cbCaptor.getValue().getContext().fallbackTopic);
		cbCaptor.getValue().onCompletion(recordMetadata, null);
		verify(cc).markCompleted();
	}

	@Test
	public void shouldNotPublishFallbackRecordIfNotConfigured() {
		KafkaCallback.ProducerContext context = makeContext();
		context.fallbackTopic = null;

		AbstractAsyncProducer.CallbackCompleter cc = mock(AbstractAsyncProducer.CallbackCompleter.class);
		RowIdentity id = new RowIdentity("a","b", null);
		KafkaCallback callback = new KafkaCallback(cc,
				new Position(new BinlogPosition(1, "binlog-1"), 0L), id, "value",
				context);

		Exception error = new RecordTooLargeException();
		RecordMetadata recordMetadata = new RecordMetadata(new TopicPartition("topic", 1), 1, 1, 1, new Long(1), 1, 1);
		callback.onCompletion(recordMetadata, error);

		verify(cc).markCompleted();
		verifyZeroInteractions(context.producer);
	}
}
