package com.zendesk.maxwell.core.springconfig;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@ComponentScan(basePackages = {"com.zendesk.maxwell.core"})
public class SpringTestContextConfiguration {
}
