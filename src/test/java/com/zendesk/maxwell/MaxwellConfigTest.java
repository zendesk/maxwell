package com.zendesk.maxwell;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.zendesk.maxwell.producer.AbstractProducer;
import com.zendesk.maxwell.producer.ProducerFactory;
import com.zendesk.maxwell.producer.StdoutProducer;

import org.junit.Test;

import java.net.URL;

import joptsimple.OptionException;

public class MaxwellConfigTest
{
    private MaxwellConfig config;
    
    @Test
    public void testFetchProducerFactoryFromArgs() {
        config = new MaxwellConfig(new String[] { "--producer_factory=" + TestProducerFactory.class.getName() });
        assertNotNull(config.producerFactory);
        assertTrue(config.producerFactory instanceof TestProducerFactory);
    }
    
    @Test
    public void testFetchProducerFactoryFromConfigFile() {
        URL configUrl = ClassLoader.getSystemResource("producer-factory-config.properties");
        config = new MaxwellConfig(new String[] { "--config=" + configUrl.getPath() });
        assertNotNull(config.producerFactory);
        assertTrue(config.producerFactory instanceof TestProducerFactory);
    }
    
    @Test(expected = OptionException.class)
    public void testCustomProperties() {
        // custom properties are not supported on the command line just like 'kafka.*' properties
        new MaxwellConfig(new String[] { "--custom.foo=bar" });
    }
    
    @Test
    public void testCustomPropertiesFromConfigFile() {
        URL configUrl = ClassLoader.getSystemResource("custom-config.properties");
        config = new MaxwellConfig(new String[] { "--config=" + configUrl.getPath() });
        assertEquals("bar", config.customProperties.getProperty("foo"));
    }
    
    public static class TestProducerFactory implements ProducerFactory {
        public AbstractProducer createProducer(MaxwellContext context) {
            return new StdoutProducer(context);
        }
    }
}

  