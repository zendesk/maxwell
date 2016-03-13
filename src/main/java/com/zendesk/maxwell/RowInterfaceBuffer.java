package com.zendesk.maxwell;

import java.io.IOException;

public interface RowInterfaceBuffer {
	public boolean isEmpty();
	public RowInterface removeFirst() throws IOException, ClassNotFoundException;
}
