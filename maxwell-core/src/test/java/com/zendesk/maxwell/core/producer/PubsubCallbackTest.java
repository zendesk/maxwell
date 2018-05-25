package com.zendesk.maxwell.core.producer;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.zendesk.maxwell.core.MaxwellContext;
import com.zendesk.maxwell.core.config.MaxwellConfig;
import com.zendesk.maxwell.core.config.MaxwellConfigFactory;
import com.zendesk.maxwell.core.replication.BinlogPosition;
import com.zendesk.maxwell.core.replication.Position;
import org.junit.Test;

import static org.mockito.Mockito.*;


public class PubsubCallbackTest {

  @Test
  public void shouldIgnoreProducerErrorByDefault() {
    MaxwellContext context = mock(MaxwellContext.class);
    MaxwellConfig config = new MaxwellConfigFactory().createNewDefaultConfiguration();
    when(context.getConfig()).thenReturn(config);
    AbstractAsyncProducer.CallbackCompleter cc = mock(AbstractAsyncProducer.CallbackCompleter.class);
    PubsubCallback callback = new PubsubCallback(cc,
      new Position(new BinlogPosition(1, "binlog-1"), 0L), "value",
      new Counter(), new Counter(), new Meter(), new Meter(), context);
    Throwable t = new Throwable("blah");
    callback.onFailure(t);
    verify(cc).markCompleted();
  }

  @Test
  public void shouldTerminateWhenNotIgnoreProducerError() {
    MaxwellContext context = mock(MaxwellContext.class);
    MaxwellConfig config = new MaxwellConfigFactory().createNewDefaultConfiguration();
    config.ignoreProducerError = false;
    when(context.getConfig()).thenReturn(config);
    AbstractAsyncProducer.CallbackCompleter cc = mock(AbstractAsyncProducer.CallbackCompleter.class);
    PubsubCallback callback = new PubsubCallback(cc,
      new Position(new BinlogPosition(1, "binlog-1"), 0L), "value",
      new Counter(), new Counter(), new Meter(), new Meter(), context);
    Throwable t = new Throwable("blah");
    callback.onFailure(t);
    verify(context).terminate(any(RuntimeException.class));
    verifyZeroInteractions(cc);
  }
}
