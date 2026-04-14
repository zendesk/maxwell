package com.zendesk.maxwell.producer;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
// Keep other Google Cloud imports: BigQuery, BigQueryOptions, Schema, Table, storage.v1.*
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.Exceptions;
import com.google.cloud.bigquery.storage.v1.Exceptions.StorageException;
import com.google.cloud.bigquery.storage.v1.JsonStreamWriter;
import com.google.cloud.bigquery.storage.v1.TableName;
import com.google.cloud.bigquery.storage.v1.TableSchema;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ThreadFactoryBuilder; // For naming threads
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.monitoring.Metrics;
import com.zendesk.maxwell.replication.Position;
import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.schema.BqToBqStorageSchemaConverter;
import com.zendesk.maxwell.util.StoppableTask;
import com.zendesk.maxwell.util.StoppableTaskState;

import io.grpc.Status;
import io.grpc.Status.Code;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Phaser;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;

import javax.annotation.concurrent.GuardedBy;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class BigQueryCallback implements ApiFutureCallback<AppendRowsResponse> {
  public final Logger LOGGER = LoggerFactory.getLogger(BigQueryCallback.class);

  private final MaxwellBigQueryProducerWorker parent;
  private final Position position;
  private MaxwellContext context;
  AppendContext appendContext;

  private Counter succeededMessageCount;
  private Counter failedMessageCount;
  private Meter succeededMessageMeter;
  private Meter failedMessageMeter;

  private static final int MAX_RETRY_COUNT = 2;
  private final ImmutableList<Code> RETRIABLE_ERROR_CODES = ImmutableList.of(Code.INTERNAL, Code.ABORTED,
      Code.CANCELLED);

  public BigQueryCallback(MaxwellBigQueryProducerWorker parent,
      AppendContext appendContext,
      Counter producedMessageCount, Counter failedMessageCount,
      Meter succeededMessageMeter, Meter failedMessageMeter,
      MaxwellContext context) {
    this.parent = parent;
    this.appendContext = appendContext;
    this.position = appendContext.position;
    this.succeededMessageCount = producedMessageCount;
    this.failedMessageCount = failedMessageCount;
    this.succeededMessageMeter = succeededMessageMeter;
    this.failedMessageMeter = failedMessageMeter;
    this.context = context;
  }

  @Override
  public void onSuccess(AppendRowsResponse response) {
    for (int i = 0; i < appendContext.callbacks.size(); i++) {
        this.succeededMessageCount.inc();
        this.succeededMessageMeter.mark();
        AbstractAsyncProducer.CallbackCompleter cc = (AbstractAsyncProducer.CallbackCompleter) appendContext.callbacks.get(i);
        cc.markCompleted();

        if (LOGGER.isDebugEnabled()) {
          try {
            LOGGER.debug("Worker {} -> {}\n", parent.getWorkerId(), this.position);
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
    }
  }

  @Override
  public void onFailure(Throwable t) {
    for (int i = 0; i < appendContext.callbacks.size(); i++) {
        this.failedMessageCount.inc();
        this.failedMessageMeter.mark();
    }

    LOGGER.error("Worker {} " + t.getClass().getSimpleName() + " @ " + position, parent.getWorkerId());
    LOGGER.error("Worker {} " + t.getLocalizedMessage(), parent.getWorkerId());

    Status status = Status.fromThrowable(t);
    if (appendContext.retryCount < MAX_RETRY_COUNT
        && RETRIABLE_ERROR_CODES.contains(status.getCode())) {
      appendContext.retryCount++;
      try {
        this.parent.attemptBatch(appendContext);
        return;
      } catch (Exception e) {
        System.out.format("Worker {} Failed to retry append: %s\n", parent.getWorkerId(), e);
      }
    }

    synchronized (this.parent.getLock()) {
      if (this.parent.getError() == null && !this.context.getConfig().ignoreProducerError) {
        StorageException storageException = Exceptions.toStorageException(t);
        this.parent.setError((storageException != null) ? storageException : new RuntimeException(t));
        context.terminate();
        return;
      }
    }
    // got an error, but we are ingoring producer error
    for (int i = 0; i < appendContext.callbacks.size(); i++) {
        AbstractAsyncProducer.CallbackCompleter cc = (AbstractAsyncProducer.CallbackCompleter) appendContext.callbacks.get(i);
        cc.markCompleted();
    }
  }
}

public class MaxwellBigQueryProducer extends AbstractProducer {
  private static final Logger LOGGER = LoggerFactory.getLogger(MaxwellBigQueryProducer.class);

  private final ArrayBlockingQueue<RowMap> queue;
  private final List<MaxwellBigQueryProducerWorker> workers;
  private final ExecutorService workerExecutor;
  private final ExecutorService callbackExecutor;

  public MaxwellBigQueryProducer(MaxwellContext context, String bigQueryProjectId,
      String bigQueryDataset, String bigQueryTable, int bigqueryThreads)
      throws IOException {
    super(context);
    bigqueryThreads = Math.max(1, bigqueryThreads);
    this.queue = new ArrayBlockingQueue<>(bigqueryThreads * MaxwellBigQueryProducerWorker.BATCH_SIZE);

    ThreadFactory workerThreadFactory = new ThreadFactoryBuilder().setNameFormat("bq-worker-%d").setDaemon(true).build();
    this.workerExecutor = Executors.newFixedThreadPool(bigqueryThreads, workerThreadFactory);

    ThreadFactory callbackThreadFactory = new ThreadFactoryBuilder().setNameFormat("bq-callback-%d").setDaemon(true).build();
    this.callbackExecutor = Executors.newCachedThreadPool(callbackThreadFactory);

    this.workers = new ArrayList<>(bigqueryThreads);
    TableName tableName = TableName.of(bigQueryProjectId, bigQueryDataset, bigQueryTable);
    startWorkers(context, tableName);
  }

  private void startWorkers(MaxwellContext context, TableName tableName) throws IOException {
    int numWorkers = this.workers.size();
    TableSchema tableSchema = getTableSchema(tableName);
     // Create and start workers
    for (int i = 0; i < Math.max(1, numWorkers); i++) {
       try {
            MaxwellBigQueryProducerWorker worker = new MaxwellBigQueryProducerWorker(
                context,
                this.queue,
                this.callbackExecutor, // Pass callback executor
                i // Pass worker ID
            );
            worker.initialize(tableName, tableSchema);
            this.workers.add(worker);
            this.workerExecutor.submit(worker);
       } catch (DescriptorValidationException | IOException | InterruptedException e) {
           LOGGER.error("Failed to initialize MaxwellBigQueryProducer worker {}: {}", i, e.getMessage(), e);
           // Don't try to shutdown executors, just throw
           throw new IOException("Failed to initialize worker " + i, e);
       }
    }
    LOGGER.info("Submitted {} workers to executor.", this.workers.size());
  }

  private TableSchema getTableSchema(TableName tName) throws IOException {
    BigQuery bigquery = BigQueryOptions.newBuilder().setProjectId(tName.getProject()).build().getService();
    Table table = bigquery.getTable(tName.getDataset(), tName.getTable());
    Schema schema = table.getDefinition().getSchema();
    // Filter out bq_inserted_at column from the schema
    List<com.google.cloud.bigquery.Field> filteredFields = schema.getFields().stream()
      .filter(field -> !"bq_inserted_at".equals(field.getName()))
      .collect(Collectors.toList());
    Schema filteredSchema = Schema.of(filteredFields);
    TableSchema tableSchema = BqToBqStorageSchemaConverter.convertTableSchema(filteredSchema);
    return tableSchema;
  }

  @Override
  public void push(RowMap r) throws Exception {
    this.queue.put(r);
  }
}
class MaxwellBigQueryProducerWorker extends AbstractAsyncProducer implements Runnable, StoppableTask {
  static final Logger LOGGER = LoggerFactory.getLogger(MaxwellBigQueryProducerWorker.class);
  public static final int BATCH_SIZE = 100;
  // checked approximately, leave a buffer
  public static final long MAX_MESSAGE_SIZE_BYTES = 5_000_000;

  private static final Map<String, Map<String, Map<String, String>>> COLUMN_ACTIONS;
  private static final List<String> CLEARED_COLUMNS;

  static {
    CLEARED_COLUMNS = List.of(
      "dp_test.test_index_created.created_at",
      "dp_test.test_index_created.updated_at",
      "dp_test.test_index_both.created_at"
    );

    Map<String, Map<String, Map<String, String>>> actions = new HashMap<>();
    // Format: database.table.column
    for (String columnPath : CLEARED_COLUMNS) {
      String[] parts = columnPath.split("\\.", 3);
      if (parts.length != 3) {
        LOGGER.warn("Skipping invalid cleared column entry: {}", columnPath);
        continue;
      }
      actions
        .computeIfAbsent(parts[0], db -> new HashMap<>())
        .computeIfAbsent(parts[1], table -> new HashMap<>())
        .put(parts[2], "clear");
    }

    COLUMN_ACTIONS = Collections.unmodifiableMap(actions);
  }



  private final ArrayBlockingQueue<RowMap> queue;
  private StoppableTaskState taskState;
  private Thread thread;
  private final Object lock = new Object();

  @GuardedBy("lock")
  private RuntimeException error = null;
  private JsonStreamWriter streamWriter;
  private final ScheduledExecutorService scheduledExecutor;
  private final ExecutorService callbackExecutor;
  private final int workerId;
  private AppendContext appendContext;

  public MaxwellBigQueryProducerWorker(MaxwellContext context,
      ArrayBlockingQueue<RowMap> queue,
      ExecutorService callbackExecutor,
      int workerId) throws IOException {
    super(context);
    this.queue = queue;
    this.callbackExecutor = callbackExecutor;
    this.workerId = workerId;
    this.scheduledExecutor = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("bq-batch-scheduler-" + workerId).setDaemon(true).build());
    Metrics metrics = context.getMetrics();
    this.taskState = new StoppableTaskState("MaxwellBigQueryProducerWorker-" + workerId); // Keep taskState init
  }

  public Object getLock() {
    return lock;
  }

  public int getWorkerId() {
    return workerId;
  }

  public RuntimeException getError() {
    return error;
  }

  public void setError(RuntimeException error) {
    this.error = error;
  }

  private void covertJSONObjectFieldsToString(JSONObject record) {
    if (this.context.getConfig().outputConfig.includesPrimaryKeys) {
      record.put("primary_key", record.get("primary_key").toString());
    }
    String data = record.has("data") == true ? record.get("data").toString() : null;
    record.put("data", data);
    String old = record.has("old") == true ? record.get("old").toString() : null;
    record.put("old", old);
  }


  public void initialize(TableName tName, TableSchema tableSchema)
      throws DescriptorValidationException, IOException, InterruptedException {
    this.streamWriter = JsonStreamWriter.newBuilder(tName.toString(), tableSchema).build();
  }

  @Override
  public void requestStop() throws Exception {
    taskState.requestStop();
    streamWriter.close();
    scheduledExecutor.shutdown();
    synchronized (this.lock) {
      if (this.error != null) {
        throw this.error;
      }
    }
  }

  @Override
  public void awaitStop(Long timeout) throws TimeoutException {
    taskState.awaitStop(thread, timeout);
  }

  @Override
  public void run() {
    this.thread = Thread.currentThread();
    while (true) {
      try {
        RowMap row = queue.take();
        if (!taskState.isRunning()) {
          taskState.stopped();
          return;
        }
        this.push(row);
      } catch (Exception e) {
        taskState.stopped();
        context.terminate(e);
        return;
      }
    }
  }

  @Override
  public void sendAsync(RowMap r, CallbackCompleter cc) throws Exception {

    JSONObject record = new JSONObject(r.toJSON(outputConfig));
    applyColumnActions(r, record);
    covertJSONObjectFieldsToString(record);

    int recordSize = AppendContext.getJsonByteSize(record);
    if (recordSize >= 9 * 1024 * 1024) {
        LOGGER.error("Worker {} skipping oversized record: {} bytes for table {}.{}, position {}",
            this.workerId, recordSize, r.getDatabase(), r.getTable(), r.getNextPosition());
        cc.markCompleted();
        return;
    }

    synchronized (this.lock) {
      if (this.error != null) {
        throw this.error;
      }

      if(this.appendContext == null) {
        this.appendContext = new AppendContext();
        this.scheduleAttempt(this.appendContext);
      }
    }

    this.appendContext.addRow(r, record, cc);

    if(this.appendContext.callbacks.size() >= BATCH_SIZE
       || this.appendContext.getApproximateSize() >= MAX_MESSAGE_SIZE_BYTES) {
        synchronized (this.getLock()) {
            this.attemptBatch(this.appendContext);
            this.appendContext = null;
        }
    }
  }

  public void attemptBatch(AppendContext appendContext) throws DescriptorValidationException, IOException {
    if(appendContext.scheduledTask != null && !appendContext.scheduledTask.isDone()) {
      appendContext.scheduledTask.cancel(false);
    }
    ApiFuture<AppendRowsResponse> future = streamWriter.append(appendContext.data);

    ApiFutures.addCallback(
        future, new BigQueryCallback(this, appendContext,
            this.succeededMessageCount, this.failedMessageCount, this.succeededMessageMeter, this.failedMessageMeter,
            this.context),
        this.callbackExecutor
    );
  }


  public void scheduleAttempt(final AppendContext appendContext) {
    appendContext.scheduledTask = this.scheduledExecutor.schedule(() -> {
      try {
        synchronized (this.getLock()) {
          this.attemptBatch(this.appendContext);
          this.appendContext = null; // Nullify after attempting via scheduler
        }
      } catch (Exception e) {
        LOGGER.error("Error sending scheduled bigquery batch message");
        e.printStackTrace();
      }
    }, 1, TimeUnit.MINUTES); // 1 minute delay
  }

  private void applyColumnActions(RowMap row, JSONObject record) {
    if (COLUMN_ACTIONS.isEmpty()) {
      return;
    }

    Map<String, Map<String, String>> databaseConfig = COLUMN_ACTIONS.get(row.getDatabase());
    if (databaseConfig == null) {
      return;
    }

    Map<String, String> tableConfig = databaseConfig.get(row.getTable());
    if (tableConfig == null || tableConfig.isEmpty()) {
      return;
    }

    applyColumnActionsToObject(record.opt("data"), tableConfig);
    applyColumnActionsToObject(record.opt("old"), tableConfig);
  }

  private void applyColumnActionsToObject(Object obj, Map<String, String> tableConfig) {
    if (!(obj instanceof JSONObject)) {
      return;
    }

    JSONObject target = (JSONObject) obj;
    for (Map.Entry<String, String> entry : tableConfig.entrySet()) {
      if ("clear".equalsIgnoreCase(entry.getValue()) && target.has(entry.getKey())) {
        target.put(entry.getKey(), JSONObject.NULL);
      }
    }
  }
}


class AppendContext {
  JSONArray data;
  int retryCount = 0;
  int records = 0;
  int approximateSize = 0;
  Position position;
  public ArrayList<AbstractAsyncProducer.CallbackCompleter> callbacks;
  public ScheduledFuture<?> scheduledTask;

  AppendContext() {
    this.data = new JSONArray();
    this.retryCount = 0;
    this.records = 0;
    this.approximateSize = 0;
    this.callbacks = new ArrayList<AbstractAsyncProducer.CallbackCompleter>();
  }

  public void addRow(RowMap r, JSONObject record, AbstractAsyncProducer.CallbackCompleter cc) {
    this.data.put(record);
    this.approximateSize += getJsonByteSize(record);
    this.callbacks.add(cc);
    if(this.position == null) {
        this.position = r.getNextPosition();
    }
  }

  public static int getJsonByteSize(Object json) {
    // Estimate byte size. UTF-8 encoding is assumed, which is standard for JSON.
    // This is an approximation; actual gRPC message size might differ slightly.
    return json.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
  }

  public int getApproximateSize() {
    return approximateSize;
  }

}
