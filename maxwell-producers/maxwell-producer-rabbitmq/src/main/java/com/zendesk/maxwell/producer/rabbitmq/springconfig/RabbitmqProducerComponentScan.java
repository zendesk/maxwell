package com.zendesk.maxwell.producer.rabbitmq.springconfig;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@ComponentScan(basePackages = "com.zendesk.maxwell.producer.rabbitmq")
public class RabbitmqProducerComponentScan {
}
