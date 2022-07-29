package com.zendesk.maxwell.producer;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.zendesk.maxwell.MaxwellConfig;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.replication.BinlogPosition;
import com.zendesk.maxwell.replication.Position;
import com.zendesk.maxwell.row.RowIdentity;
import com.zendesk.maxwell.row.RowMap;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import org.apache.commons.lang3.tuple.Pair;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;
import com.zendesk.maxwell.monitoring.NoOpMetrics;

import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ArrayBlockingQueue;

public class BigQueryCallbackTest {

    @Test
    public void shouldIgnoreProducerErrorByDefault() throws JSONException, Exception {
        MaxwellContext context = mock(MaxwellContext.class);
        MaxwellConfig config = new MaxwellConfig();
        when(context.getConfig()).thenReturn(config);
        when(context.getMetrics()).thenReturn(new NoOpMetrics());
        MaxwellOutputConfig outputConfig = new MaxwellOutputConfig();
		outputConfig.includesServerId = true;
        RowMap r = new RowMap("insert", "MyDatabase", "MyTable", 1234567890L, new ArrayList<String>(), null);
        JSONArray jsonArr = new JSONArray();
        JSONObject record = new JSONObject(r.toJSON(outputConfig));
        jsonArr.put(record);
        AbstractAsyncProducer.CallbackCompleter cc = mock(AbstractAsyncProducer.CallbackCompleter.class);
        AppendContext appendContext = new AppendContext(jsonArr, 0, r);
        ArrayBlockingQueue<RowMap> queue =  new ArrayBlockingQueue<RowMap>(100);
        MaxwellBigQueryProducerWorker producerWorker = new MaxwellBigQueryProducerWorker(context, queue,"myproject", "mydataset", "mytable");
        BigQueryCallback callback = new BigQueryCallback(producerWorker, appendContext, cc,
                new Position(new BinlogPosition(1, "binlog-1"), 0L),
                new Counter(), new Counter(), new Meter(), new Meter(), context);
        Throwable t = new Throwable("error");
        callback.onFailure(t);
        verify(cc).markCompleted();
    }
}
