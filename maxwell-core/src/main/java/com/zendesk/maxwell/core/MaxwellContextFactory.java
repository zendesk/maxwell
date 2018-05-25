package com.zendesk.maxwell.core;

import com.zendesk.maxwell.core.config.MaxwellConfig;
import org.springframework.stereotype.Service;

import java.net.URISyntaxException;
import java.sql.SQLException;

@Service
public class MaxwellContextFactory {
	public MaxwellContext createFor(MaxwellConfig maxwellConfig) throws SQLException, URISyntaxException {
		MaxwellContext context = new MaxwellContext(maxwellConfig);
		context.probeConnections();
		return context;
	}
}
