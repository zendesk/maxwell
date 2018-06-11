package com.zendesk.maxwell.metricreporter.http.springconfig;

import com.zendesk.maxwell.standalone.api.MaxwellRuntime;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class MaxwellRuntimeConfig {

    @Bean
    public MaxwellRuntime maxwellRuntime(){
        return new MaxwellRuntime() {
            @Override
            public void shutdown() {

            }

            @Override
            public void shutdown(Exception e) {

            }
        };
    }

}
