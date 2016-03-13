package com.zendesk.maxwell;

import com.zendesk.maxwell.schema.ddl.DDLRow;
import java.io.IOException;
import java.util.LinkedList;

public class DDLRowBuffer implements RowInterfaceBuffer {
	private final LinkedList<DDLRow> buffer;

	public DDLRowBuffer() {
		buffer = new LinkedList<>();
	}

	public boolean isEmpty() {
		return buffer.isEmpty();
	}
	public void add(DDLRow d) {
		buffer.add(d);
	}

	public DDLRow removeFirst() throws IOException, ClassNotFoundException {
		return buffer.removeFirst();
	}
}
