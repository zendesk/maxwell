package com.zendesk.maxwell.producer.sqs.springconfig;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@ComponentScan(basePackages = "com.zendesk.maxwell.producer.sqs")
public class SQSProducerComponentScan {
}
