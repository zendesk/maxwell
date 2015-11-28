package com.zendesk.maxwell;

import com.zendesk.maxwell.util.ListWithDiskBuffer;

import java.io.IOException;

public class RowMapBuffer extends ListWithDiskBuffer<RowMap> {
	private Long xid;

	public RowMapBuffer(long maxInMemoryElements) throws IOException {
		super(maxInMemoryElements);
	}

	public RowMap removeFirst() throws IOException, ClassNotFoundException {
		RowMap r = super.removeFirst();
		r.setXid(this.xid);
		return r;
	}

	public void setXid(Long xid) {
		this.xid = xid;
	}
}
