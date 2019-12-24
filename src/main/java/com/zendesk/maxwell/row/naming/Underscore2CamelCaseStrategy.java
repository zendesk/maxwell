package com.zendesk.maxwell.row.naming;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 
 * @author frank chen
 * @Date Dec 24,2019 8:53:17 PM
 */
public class Underscore2CamelCaseStrategy implements INamingStrategy {

    static Underscore2CamelCaseStrategy instance = new Underscore2CamelCaseStrategy();
    
    //cache converted names
    private Map<String, String> caches = new ConcurrentHashMap<>();
    
    @Override
    public String apply(String oldName) {
        return caches.computeIfAbsent(oldName, k -> toCamelCase(k));
    }

    private String toCamelCase(String oldName) {
        StringBuilder builder = new StringBuilder(oldName.length());
        for (int i = 0, len = oldName.length(); i < len; i++) {
            char c = oldName.charAt(i);
            if ( c == '_' && i + 1 < len ) {
                //
                //turn the char after underscore to be upper case
                //
                char n = oldName.charAt(i);
                if ( n >= 'a' && n <= 'z' )
                    n = (char)(n - 'a' + 'A');
                
                builder.append(n);
                i++;
            } else {
                builder.append(c);
            }
        }
        return builder.toString();
    }
}
