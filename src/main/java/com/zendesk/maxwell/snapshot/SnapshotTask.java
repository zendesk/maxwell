package com.zendesk.maxwell.snapshot;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.schema.Table;

public class SnapshotTask {

	static private final int DefaultChunkSize = 200;

	private final String id;
	private final String db;
	private final String table;
	private final String whereClause;
	private final String requestComment;
	private final Table tableSchema;
	private String lowWatermark;
	private String highWatermark;
	private boolean inRange;
	private int chunkStart;
	private int chunkSize;
	private Map<String, RowMap> chunk;
	private boolean complete;
	private boolean successful;
	private String completionReason;
	private int rowsSent;
	private Instant createdAt;
	private Instant completedAt;

	public SnapshotTask(String id, String db, String table, String whereClause, Table tableSchema, String requestComment,
			Instant createdAt) {
		this.id = id;
		this.db = db;
		this.table = table;
		this.whereClause = whereClause;
		this.tableSchema = tableSchema;
		this.requestComment = requestComment;
		chunkSize = DefaultChunkSize;
		chunk = new LinkedHashMap<String, RowMap>();
		this.createdAt = createdAt;
	}

	public String getLowWatermark() {
		return lowWatermark;
	}

	public void setLowWatermark(String lowWatermark) {
		this.lowWatermark = lowWatermark;
	}

	public String getHighWatermark() {
		return highWatermark;
	}

	public void setHighWatermark(String highWatermark) {
		this.highWatermark = highWatermark;
	}

	public boolean isInRange() {
		return inRange;
	}

	public void setInRange(boolean inRange) {
		this.inRange = inRange;
	}

	public int getChunkStart() {
		return chunkStart;
	}

	public void setChunkStart(int chunkStart) {
		this.chunkStart = chunkStart;
	}

	public int getChunkSize() {
		return chunkSize;
	}

	public void setChunkSize(int chunkSize) {
		this.chunkSize = chunkSize;
	}

	public boolean isComplete() {
		return complete;
	}

	public void setComplete(boolean complete) {
		this.complete = complete;
	}

	public Instant getCreatedAt() {
		return createdAt;
	}

	public void setCreatedAt(Instant created) {
		this.createdAt = created;
	}

	public Instant getCompletedAt() {
		return completedAt;
	}

	public void setCompletedAt(Instant completed) {
		this.completedAt = completed;
	}

	public String getId() {
		return id;
	}

	public String getDb() {
		return db;
	}

	public String getTable() {
		return table;
	}

	public String getFullTableName() {
		return db + "." + table;
	}

	public String getWhereClause() {
		return whereClause;
	}

	public Table getTableSchema() {
		return tableSchema;
	}

	public Map<String, RowMap> getChunk() {
		return chunk;
	}

	public boolean isSuccessful() {
		return successful;
	}

	public void setSuccessful(boolean successful) {
		this.successful = successful;
	}

	public String getCompletionReason() {
		return completionReason;
	}

	public void setCompletionReason(String completionReason) {
		this.completionReason = completionReason;
	}

	public int getRowsSent() {
		return rowsSent;
	}

	public void setRowsSent(int rowsSent) {
		this.rowsSent = rowsSent;
	}

	public String getRequestComment() {
		return requestComment;
	}

	public void increaseChunkStart(int amount) {
		chunkStart += amount;
	}

	public void increaseRowsSent(int amount) {
		rowsSent += amount;
	}

	public void complete() {
		complete(true, null);
	}

	public void complete(boolean successful, String reason) {
		this.complete = true;
		this.successful = successful;
		this.completionReason = reason;
		this.completedAt = Instant.now();
	}
}