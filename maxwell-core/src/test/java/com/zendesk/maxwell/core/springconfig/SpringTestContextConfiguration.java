package com.zendesk.maxwell.core.springconfig;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import({CoreComponentScanConfig.class})
public class SpringTestContextConfiguration {

}
