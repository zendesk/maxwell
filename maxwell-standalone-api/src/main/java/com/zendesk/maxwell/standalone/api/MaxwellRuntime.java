package com.zendesk.maxwell.standalone.api;

public interface MaxwellRuntime {
    void shutdown();
    void shutdown(Exception e);
}
