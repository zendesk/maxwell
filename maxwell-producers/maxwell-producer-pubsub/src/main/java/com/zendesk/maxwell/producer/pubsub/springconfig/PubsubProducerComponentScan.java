package com.zendesk.maxwell.producer.pubsub.springconfig;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@ComponentScan(basePackages = "com.zendesk.maxwell.producer.pubsub")
public class PubsubProducerComponentScan {
}
