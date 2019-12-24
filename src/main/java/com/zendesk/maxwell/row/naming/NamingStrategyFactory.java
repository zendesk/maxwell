package com.zendesk.maxwell.row.naming;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 
 * @author frankchen
 * @Date Dec 24, 2019 9:08:41 PM
 */
public class NamingStrategyFactory {

    static class CacheStrategy implements INamingStrategy {
        //cache converted names
        private Map<String, String> caches = new ConcurrentHashMap<>();

        INamingStrategy strategy;
        public void setStrategy(INamingStrategy strategy) {
            this.strategy = strategy;
        }
        @Override
        public String apply(String oldName) {
            return caches.computeIfAbsent(oldName, k -> strategy.apply(k));
        }
    }
    
    static class NoneStrategy implements INamingStrategy {
        public static INamingStrategy INSTANCE = new NoneStrategy();
        
        @Override
        public String apply(String oldName) {
            return oldName;
        }
    }
    
    private static CacheStrategy cacheStrategy = new CacheStrategy();
    
    public static INamingStrategy create(String name) {
        if ("underscore_to_camelcase".equals(name)) {
            cacheStrategy.setStrategy(new Underscore2CamelCaseStrategy());
            return cacheStrategy;
        }

        return NoneStrategy.INSTANCE;
    }
}
