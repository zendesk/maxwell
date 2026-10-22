package com.zendesk.maxwell.schema.ddl;

import java.io.Serial;

class MaxwellSQLSyntaxError extends RuntimeException {
	@Serial private static final long serialVersionUID = 140545518818187219L;

	public MaxwellSQLSyntaxError(String message) {
		super(message);
	}
}
