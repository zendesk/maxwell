package com.zendesk.maxwell.row;

import com.zendesk.maxwell.util.ListWithDiskBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class RowMapBuffer extends ListWithDiskBuffer<RowMap> {
	static final Logger LOGGER = LoggerFactory.getLogger(RowMapBuffer.class);
	private static long FlushOutputStreamBytes = 10000000;
	private Long xid;
	private Long xoffset = 0L;
	private Long serverId;
	private Long threadId;
	private Long schemaId;
	private long memorySize = 0;
	private long outputStreamCacheSize = 0;
	private final long maxMemory;

	public RowMapBuffer(long maxInMemoryElements) {
		super(maxInMemoryElements);
		this.maxMemory = (long) (Runtime.getRuntime().maxMemory() * 0.25);
	}

	public RowMapBuffer(long maxInMemoryElements, long maxMemory) {
		super(maxInMemoryElements);
		this.maxMemory = maxMemory;
	}

	public RowMapBuffer(long maxInMemoryElements, float bufferMemoryUsage) {
		super(maxInMemoryElements);
		this.maxMemory = (long) (Runtime.getRuntime().maxMemory() * bufferMemoryUsage);
	}

	@Override
	public void add(RowMap rowMap) throws IOException {
		this.memorySize += rowMap.getApproximateSize();
		super.add(rowMap);
	}

	@Override
	protected boolean shouldBuffer() {
		return memorySize > maxMemory;
	}

	@Override
	protected RowMap evict() throws IOException {
		RowMap r = super.evict();
		this.memorySize -= r.getApproximateSize();

		/* For performance reasons, the output stream will hold on to cached objects.
		 * There's probably a smarter thing to do (write our own serdes, maybe?), but
		 * for now we forcibly flush its cache when it gets too big. */
		this.outputStreamCacheSize += r.getApproximateSize();
		if ( this.outputStreamCacheSize > FlushOutputStreamBytes ) {
			resetOutputStreamCaches();
			LOGGER.debug("outputStreamCacheSize: {}, memorySize: {}", this.outputStreamCacheSize, this.memorySize);
			this.outputStreamCacheSize = 0;
		}

		return r;
	}

	public RowMap removeFirst() throws IOException, ClassNotFoundException {
		RowMap r = super.removeFirst(RowMap.class);
		r.setXid(this.xid);
		r.setXoffset(this.xoffset++);
		r.setServerId(this.serverId);
		r.setThreadId(this.threadId);
		r.setSchemaId(this.schemaId);

		return r;
	}

	public void setXid(Long xid) {
		this.xid = xid;
	}

	public void setServerId(Long serverId) {
		this.serverId = serverId;
	}

	public void setThreadId(Long threadId) {
		this.threadId = threadId;
	}

	public void setSchemaId(Long schemaId) {
		this.schemaId = schemaId;
	}
}
