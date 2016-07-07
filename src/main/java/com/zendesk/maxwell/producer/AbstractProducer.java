package com.zendesk.maxwell.producer;

import java.sql.SQLException;

import com.zendesk.maxwell.BinlogPosition;
import com.zendesk.maxwell.MaxwellAbstractRowsEvent;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.RowInterface;

public abstract class AbstractProducer {
	protected final MaxwellContext context;

	public AbstractProducer(MaxwellContext context) {
		this.context = context;
	}

	abstract public void push(RowInterface r) throws Exception;

	public void writePosition(BinlogPosition p) throws SQLException {
		this.context.setPosition(p);
	}
}
