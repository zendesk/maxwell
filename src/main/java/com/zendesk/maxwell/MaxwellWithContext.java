package com.zendesk.maxwell;

import java.sql.SQLException;

public class MaxwellWithContext extends Maxwell {

	public MaxwellWithContext(MaxwellContext context) throws SQLException {
		super(context);
	}

}
