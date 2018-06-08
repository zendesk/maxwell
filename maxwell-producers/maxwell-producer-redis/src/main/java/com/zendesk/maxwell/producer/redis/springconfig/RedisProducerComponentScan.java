package com.zendesk.maxwell.producer.redis.springconfig;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@ComponentScan(basePackages = "com.zendesk.maxwell.producer.redis")
public class RedisProducerComponentScan {
}
