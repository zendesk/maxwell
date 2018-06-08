package com.zendesk.maxwell.core.springconfig;

import com.zendesk.maxwell.test.springconfig.TestSupportComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import({CoreComponentScanConfig.class,TestSupportComponentScan.class})
public class SpringTestContextConfiguration {

}
