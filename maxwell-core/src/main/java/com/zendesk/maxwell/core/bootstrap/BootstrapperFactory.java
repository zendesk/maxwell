package com.zendesk.maxwell.core.bootstrap;

import com.zendesk.maxwell.core.MaxwellContext;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
public class BootstrapperFactory {

	public Bootstrapper createFor(MaxwellContext maxwellContext) throws IOException {
		switch (maxwellContext.getConfig().getBootstrapperType()) {
			case "async":
				return new AsynchronousBootstrapper(maxwellContext);
			case "sync":
				return new SynchronousBootstrapper(maxwellContext);
			default:
				return new NoOpBootstrapper(maxwellContext);
		}

	}

}
