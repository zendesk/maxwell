package com.zendesk.maxwell.core;

import com.zendesk.maxwell.api.MaxwellContext;

public interface ContextStartListener {
	void onContextStart(MaxwellContext context);
}
