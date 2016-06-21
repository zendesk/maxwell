package com.zendesk.maxwell;

import com.zendesk.maxwell.util.ListWithDiskBuffer;

import java.io.IOException;

public class RowMapBuffer extends ListWithDiskBuffer<RowMap> {
	private static long FlushOutputStreamBytes = 10000000;
	private Long xid;
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
			this.outputStreamCacheSize = 0;
		}

		return r;
	}

	public RowMap removeFirst() throws IOException, ClassNotFoundException {
		RowMap r = super.removeFirst(RowMap.class);
		r.setXid(this.xid);

		return r;
	}

	public void setXid(Long xid) {
		this.xid = xid;
	}
}
