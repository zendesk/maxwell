package com.zendesk.maxwell.producer.kafka;

import com.zendesk.maxwell.core.MaxwellContext;
import com.zendesk.maxwell.core.monitoring.MaxwellDiagnostic;
import com.zendesk.maxwell.core.monitoring.MaxwellDiagnosticResult;
import com.zendesk.maxwell.core.row.RowMap;
import com.zendesk.maxwell.core.row.RowMapFactory;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class KafkaProducerDiagnostic implements MaxwellDiagnostic {

	private final MaxwellKafkaProducerWorker kafkaProducerWorker;
	private final KafkaProducerConfiguration producerConfiguration;
	private final MaxwellContext context;
	private final RowMapFactory rowMapFactory;

	public KafkaProducerDiagnostic(MaxwellKafkaProducerWorker kafkaProducerWorker, KafkaProducerConfiguration configuration, MaxwellContext context, RowMapFactory rowMapFactory) {
		this.kafkaProducerWorker = kafkaProducerWorker;
		this.producerConfiguration = configuration;
		this.context = context;
		this.rowMapFactory = rowMapFactory;
	}

	@Override
	public String getName() {
		return "kafka-producer";
	}

	@Override
	public CompletableFuture<MaxwellDiagnosticResult.Check> check() {
		return getLatency().thenApply(this::normalResult).exceptionally(this::exceptionResult);
	}

	@Override
	public boolean isMandatory() {
		return true;
	}

	@Override
	public String getResource() {
		return producerConfiguration.getKafkaProperties().getProperty("bootstrap.servers");
	}

	public CompletableFuture<Long> getLatency() {
		DiagnosticCallback callback = new DiagnosticCallback();
		try {
			RowMap rowMap = rowMapFactory.createFor("insert", context.getConfig().databaseName, "dummy", System.currentTimeMillis(),
					new ArrayList<>(), context.getPosition());
			rowMap.setTXCommit();
			ProducerRecord<String, String> record = kafkaProducerWorker.makeProducerRecord(rowMap);
			kafkaProducerWorker.sendAsync(record, callback);
		} catch (Exception e) {
			callback.latency.completeExceptionally(e);
		}
		return callback.latency;
	}

	private MaxwellDiagnosticResult.Check normalResult(Long latency) {
		Map<String, String> info = new HashMap<>();
		info.put("message", "Kafka producer acknowledgement lag is " + latency.toString() + "ms");
		return new MaxwellDiagnosticResult.Check(this, true, info);
	}

	private MaxwellDiagnosticResult.Check exceptionResult(Throwable e) {
		Map<String, String> info = new HashMap<>();
		info.put("error", e.getCause().toString());
		return new MaxwellDiagnosticResult.Check(this, false, info);
	}

	static class DiagnosticCallback implements Callback {
		final CompletableFuture<Long> latency;
		final long sendTime;

		DiagnosticCallback() {
			latency = new CompletableFuture<>();
			sendTime = System.currentTimeMillis();
		}

		@Override
		public void onCompletion(final RecordMetadata metadata, final Exception exception) {
			if (exception == null) {
				latency.complete(System.currentTimeMillis() - sendTime);
			} else {
				latency.completeExceptionally(exception);
			}
		}
	}
}
