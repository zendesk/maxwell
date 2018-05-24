package com.zendesk.maxwell.core;

import java.net.URISyntaxException;
import java.sql.SQLException;

public class MaxwellWithContextRunner extends MaxwellRunner {

	public MaxwellWithContextRunner(MaxwellContext context) throws SQLException, URISyntaxException {
		super(context);
	}

}
