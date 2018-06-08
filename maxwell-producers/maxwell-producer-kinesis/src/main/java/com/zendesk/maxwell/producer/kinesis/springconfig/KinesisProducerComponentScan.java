package com.zendesk.maxwell.producer.kinesis.springconfig;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@ComponentScan(basePackages = "com.zendesk.maxwell.producer.kinesis")
public class KinesisProducerComponentScan {

}
