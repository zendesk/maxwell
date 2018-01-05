package com.zendesk.maxwell.example.producerfactory;

import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.producer.AbstractProducer;
import com.zendesk.maxwell.producer.ProducerFactory;

/**
 * Custom {@link ProducerFactory} example that creates a new {@link CustomProducer}. To register your custom producer, 
 * set the {@code custom_producer.factory} property to your custom producer factory's fully qualified class name.
 */
public class CustomProducerFactory implements ProducerFactory
{
	@Override
	public AbstractProducer createProducer(MaxwellContext context)
	{
		return new CustomProducer(context);
	}
}
