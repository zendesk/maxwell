package com.zendesk.maxwell.api;

import java.util.Properties;

public interface MaxwellLauncher {
	MaxwellContext launch(Properties configurationProperties) throws Exception;
}
