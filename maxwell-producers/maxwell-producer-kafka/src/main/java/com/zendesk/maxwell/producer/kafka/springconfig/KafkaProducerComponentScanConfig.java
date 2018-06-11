package com.zendesk.maxwell.producer.kafka.springconfig;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@ComponentScan(basePackages = "com.zendesk.maxwell.producer.kafka")
public class KafkaProducerComponentScanConfig {
}
