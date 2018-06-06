package com.zendesk.maxwell.core.replication;

import com.zendesk.maxwell.api.config.MaxwellMysqlConfig;
import com.zendesk.maxwell.core.MaxwellSystemContext;
import com.zendesk.maxwell.api.monitoring.MaxwellDiagnostic;
import com.zendesk.maxwell.api.monitoring.MaxwellDiagnosticResult;

import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import java.util.Observable;
import java.util.Observer;
import java.util.concurrent.CompletableFuture;

public class BinlogConnectorDiagnostic implements MaxwellDiagnostic {

	private final MaxwellSystemContext context;

	public BinlogConnectorDiagnostic(MaxwellSystemContext context) {
			this.context = context;
	}

	@Override
	public String getName() {
		return "binlog-connector";
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
		MaxwellMysqlConfig mysql = context.getConfig().getMaxwellMysql();
		return mysql.getHost() + ":" + mysql.getPort();
	}

	public CompletableFuture<Long> getLatency() {
		HeartbeatObserver observer = new HeartbeatObserver(context.getHeartbeatNotifier(), Clock.systemUTC());
		try {
			context.heartbeat();
		} catch (Exception e) {
			observer.fail(e);
		}

		return observer.latency;
	}

	private MaxwellDiagnosticResult.Check normalResult(Long latency) {
		Map<String, String> info = new HashMap<>();
		info.put("message", "Binlog replication lag is " + latency.toString() + "ms");
		return new MaxwellDiagnosticResult.Check(this, true, info);
	}

	private MaxwellDiagnosticResult.Check exceptionResult(Throwable e) {
		Map<String, String> info = new HashMap<>();
		info.put("error", e.getCause().toString());
		return new MaxwellDiagnosticResult.Check(this, false, info);
	}

	static class HeartbeatObserver implements Observer {
		final CompletableFuture<Long> latency;
		private final HeartbeatNotifier notifier;
		private final Clock clock;

		HeartbeatObserver(HeartbeatNotifier notifier, Clock clock) {
			this.notifier = notifier;
			this.clock = clock;
			this.latency = new CompletableFuture<>();
			this.latency.whenComplete((value, exception) -> close());
			notifier.addObserver(this);
		}

		@Override
		public void update(Observable o, Object arg) {
			long heartbeatReadTime = clock.millis();
			long latestHeartbeat = (long) arg;
			latency.complete(heartbeatReadTime - latestHeartbeat);
		}

		void fail(Exception e) {
			latency.completeExceptionally(e);
		}

		private void close() {
			notifier.deleteObserver(this);
		}
	}
}
