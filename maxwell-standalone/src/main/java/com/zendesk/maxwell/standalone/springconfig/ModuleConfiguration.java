package com.zendesk.maxwell.standalone.springconfig;

import com.zendesk.maxwell.core.springconfig.CoreComponentScanConfig;
import com.zendesk.maxwell.metricreporter.core.springconfig.MetricsReporterCoreComponentScanConfig;
import com.zendesk.maxwell.metricreporter.datadog.springconfig.DatadogMetricReporterComponentScanConfig;
import com.zendesk.maxwell.metricreporter.http.springconfig.HttpMetricReporterComponentScanConfig;
import com.zendesk.maxwell.producer.kafka.springconfig.KafkaProducerComponentScanConfig;
import com.zendesk.maxwell.producer.kinesis.springconfig.KinesisProducerComponentScan;
import com.zendesk.maxwell.producer.pubsub.springconfig.PubsubProducerComponentScan;
import com.zendesk.maxwell.producer.rabbitmq.springconfig.RabbitmqProducerComponentScan;
import com.zendesk.maxwell.producer.redis.springconfig.RedisProducerComponentScan;
import com.zendesk.maxwell.producer.sqs.springconfig.SQSProducerComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import({
		CoreComponentScanConfig.class,
		KafkaProducerComponentScanConfig.class,
		KinesisProducerComponentScan.class,
		PubsubProducerComponentScan.class,
		RabbitmqProducerComponentScan.class,
		RedisProducerComponentScan.class,
		SQSProducerComponentScan.class,
		MetricsReporterCoreComponentScanConfig.class,
		HttpMetricReporterComponentScanConfig.class,
		DatadogMetricReporterComponentScanConfig.class
})
public class ModuleConfiguration {
}
