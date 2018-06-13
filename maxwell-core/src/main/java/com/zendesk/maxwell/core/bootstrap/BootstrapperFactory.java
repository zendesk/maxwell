package com.zendesk.maxwell.core.bootstrap;

import com.zendesk.maxwell.core.row.RowMapFactory;
import com.zendesk.maxwell.core.MaxwellContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
public class BootstrapperFactory {

	private final RowMapFactory rowMapFactory;

	@Autowired
	public BootstrapperFactory(RowMapFactory rowMapFactory) {
		this.rowMapFactory = rowMapFactory;
	}

	public Bootstrapper createFor(MaxwellContext maxwellContext) throws IOException {
		switch (maxwellContext.getConfig().getBootstrapperType()) {
			case "async":
				return new AsynchronousBootstrapper(maxwellContext, rowMapFactory);
			case "sync":
				return new SynchronousBootstrapper(maxwellContext, rowMapFactory);
			default:
				return new NoOpBootstrapper(maxwellContext, rowMapFactory);
		}

	}

}
