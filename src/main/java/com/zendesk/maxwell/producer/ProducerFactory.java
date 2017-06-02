/**
 * Created By aditya
 * Created On 02-Jun-2017
 */
package com.zendesk.maxwell.producer;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zendesk.maxwell.MaxwellConfig;
import com.zendesk.maxwell.MaxwellContext;

/**
 * 
 * @author aditya
 *
 */
public class ProducerFactory {
    static final Logger LOGGER = LoggerFactory.getLogger(ProducerFactory.class);

    private static ProducerFactory _instance = new ProducerFactory();
    private Map<String, Class<AbstractProducer>> producerTypeMap = new HashMap<>();

    public static ProducerFactory getInstance() {
        return _instance;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void addProducer(String producerName, Class<?> producerClass) {
        LOGGER.info("registering producer: {} {}", producerName, producerClass);
        producerTypeMap.put(producerName, (Class) producerClass);
    }

    public AbstractProducer getProducer(MaxwellContext context, MaxwellConfig config) {
        Class<AbstractProducer> producerClazz = producerTypeMap.get(config.producerType);
        if (null == producerClazz) {
            LOGGER.error("could not find producer: {}", config.producerType);
            throw new RuntimeException("could not find: " + config.producerType);
        }
        try {
            LOGGER.info("attempting to instantiate producer: {}", config.producerType);
            Constructor<AbstractProducer> constructor =
                    producerClazz.getConstructor(MaxwellContext.class, MaxwellConfig.class);
            AbstractProducer newInstance = constructor.newInstance(context, config);
            LOGGER.info("instantiated producer of type: {}", config.producerType);
            return newInstance;
        } catch (InstantiationException | IllegalAccessException | IllegalArgumentException
                | InvocationTargetException e) {
            LOGGER.error("could not instantiate producer:", e);
            e.printStackTrace();
        } catch (NoSuchMethodException | SecurityException e) {
            LOGGER.error("could not access producer:", e);
            e.printStackTrace();
        }
        throw new RuntimeException("unknown producer type: " + config.producerType);
    }

}
