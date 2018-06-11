package com.zendesk.maxwell.api.config;

public interface ModuleConfiguration {
    default void mergeWith(MaxwellConfig config){}
    default void validate(){}
}
