package com.zendesk.maxwell.core;

import com.zendesk.maxwell.core.config.MaxwellConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.stereotype.Service;

import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.List;

@Service
public class MaxwellContextFactory {
	private final List<ContextStartListener> contextStartListeners;

	@Autowired
	public MaxwellContextFactory(List<ContextStartListener> contextStartListeners) {
		this.contextStartListeners = contextStartListeners;
	}

	public MaxwellContext createFor(MaxwellConfig maxwellConfig) throws SQLException, URISyntaxException {
		MaxwellContext context = new MaxwellContext(maxwellConfig, contextStartListeners);
		context.probeConnections();
		return context;
	}
}
